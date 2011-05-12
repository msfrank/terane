/*
 * Copyright 2010,2011 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Terane.
 *
 * Terane is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * Terane is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Terane.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "storage.h"

/*
 * _Env_dealloc: free resources for the Env object.
 */
static void
_Env_dealloc (terane_Env *self)
{
    terane_Env_close (self, NULL);
    if (self->logger)
        Py_DECREF (self->logger);
    self->logger = NULL;
    self->ob_type->tp_free ((PyObject *) self);
}

/*
 * _Env_checkpoint_thread: perform a checkpoint every minute.
 */
static void *
_Env_checkpoint_thread (void *ptr)
{
    terane_Env *env = (terane_Env *) ptr;
    int dbret;

    /* enable deferred cancellation */
    pthread_setcancelstate (PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype (PTHREAD_CANCEL_DEFERRED, NULL);
    /* loop performing a checkpoint once a minute */
    for (;;) {
        sleep (60);
        dbret = env->env->txn_checkpoint (env->env, 0, 0, 0);
        if (dbret != 0)
            log_msg (TERANE_LOG_ERROR, "terane.db.storage", "txn_checkpoint failed: %s",
                db_strerror (dbret));
        pthread_testcancel ();
    }
    return NULL;
}

/*
 * terane_Env_new: allocate a new Env object.
 *
 * callspec: Env(envdir, datadir, tmpdir, [cachesize, [logger]])
 * parameters:
 *  envdir (string): A path to the DB environment
 *  datadir (string): A path to where the database data is stored
 *  tmpdir (string): A path to a directory used for temporary data
 *  cachesize (int): The size of the database cache
 *  logger (logging.Logger): A logger instance to log to
 * returns: A new Env object
 * exceptions:
 *  Exception: failed to create the DB_ENV handle
 */
PyObject *
terane_Env_new (PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    terane_Env *self;
    char *kwlist[] = {"envdir", "datadir", "tmpdir", "cachesize", "logger", NULL};
    char *envdir = NULL, *datadir = NULL, *tmpdir = NULL;
    unsigned int cachesize = 0;
    int dbret;

    /* allocate the Env object */
    self = (terane_Env *) type->tp_alloc (type, 0);
    if (self == NULL)
        return NULL;
    /* create the DB_ENV handle */
    dbret = db_env_create (&self->env, 0);
    if (dbret != 0) {
        PyErr_Format (PyExc_Exception, "Failed to create DB_ENV: %s", db_strerror (dbret));
        goto error;
    }
    /* parse constructor parameters */
    if (!PyArg_ParseTupleAndKeywords (args, kwds, "sss|IO",
        kwlist, &envdir, &datadir, &tmpdir, &cachesize, &self->logger))
        goto error;
    /* add a reference to the logger object */
    if (self->logger != NULL)
        Py_INCREF (self->logger);
    /* set the data_dir.  datadir string should not be freed. */
    dbret = self->env->set_data_dir (self->env, datadir);
    if (dbret != 0) {
        PyErr_Format (PyExc_Exception, "Failed to set datadir: %s", db_strerror (dbret));
        goto error;
    }
    /* set the tmp dir.  the tmpdir string should not be freed. */
    dbret = self->env->set_tmp_dir (self->env, tmpdir);
    if (dbret != 0) {
        PyErr_Format (PyExc_Exception, "Failed to set tmpdir: %s", db_strerror (dbret));
        goto error;
    }
    /* if the cachesize was specified, then set it, otherwise use the default (256kb) */
    if (cachesize > 0) {
        dbret = self->env->set_cachesize (self->env, 0, cachesize, 0);
        if (dbret != 0) {
            PyErr_Format (PyExc_Exception, "Failed to set cachesize: %s", db_strerror (dbret));
            goto error;
        }
    }
    /* open the database environment */
    dbret = self->env->open (self->env, envdir, DB_CREATE | DB_INIT_TXN |
        DB_INIT_MPOOL | DB_INIT_LOCK | DB_INIT_LOG | DB_REGISTER | DB_RECOVER, 0);
    if (dbret != 0) {
        PyErr_Format (PyExc_Exception, "Failed to open environment: %s",
            db_strerror (dbret));
        goto error;
    }
    /* start the checkpoint thread */
    dbret = pthread_create (&self->checkpoint_thread, NULL, _Env_checkpoint_thread, self);
    if (dbret != 0) {
        PyErr_Format (PyExc_Exception, "Failed to start checkpoint thread: %s",
            strerror (dbret));
    }

    return (PyObject *) self;

/* if there was an error, clean up and bail out */
error:
    if (self->logger)
        Py_DECREF (self->logger);
    if (self)
        _Env_dealloc ((terane_Env *) self);
    return NULL;
}

/*
 * terane_Env_close: close the underlying DB_ENV handle.
 *
 * callspec: Env.close()
 * parameters: None
 * returns: None
 * exceptions:
 *  Exception: failed to close the DB_ENV handle
 */
PyObject *
terane_Env_close (terane_Env *self, PyObject *args)
{
    int dbret;

    if (self->checkpoint_thread > 0) {
        /* cancel the checkpoint thread */
        pthread_cancel (self->checkpoint_thread);
        /* wait for the checkpoint thread to finish */
        pthread_join (self->checkpoint_thread, NULL);
        self->checkpoint_thread = 0;
    }
    if (self->env != NULL) {
        /* close the DB environment */
        dbret = self->env->close (self->env, 0);
        if (dbret != 0)
            PyErr_Format (PyExc_Exception, "Failed to close environment: %s", db_strerror (dbret));
        self->env = NULL;
    }
    Py_RETURN_NONE;
}

/*
 * terane_Env_log: log a message.
 */
void
Env_log (terane_Env *env, int level, const char *fmt, ...)
{
    va_list ap;
    PyObject *message = NULL, *ret;

    /* if no logger is present, then just return */
    if (env->logger == NULL)
        return;
    /* build a python string from the format string and var-args */
    va_start (ap, fmt);
    message = PyString_FromFormatV (fmt, ap);
    va_end (ap);
    /* call the log function with the supplied arguments */
    ret = PyObject_CallMethod (env->logger, "log", "iO", level, message);
    /* unreference objects */
    Py_XDECREF (ret);
    Py_XDECREF (message);
}

/* Env methods declaration */
PyMethodDef _Env_methods[] =
{
    { "close", (PyCFunction) terane_Env_close, METH_NOARGS, "Close the DB Environment." },
    { NULL, NULL, 0, NULL }
};

/* Env type declaration */
PyTypeObject terane_EnvType = {
    PyObject_HEAD_INIT(NULL)
    0,
    "storage.Env",
    sizeof (terane_Env),
    0,                         /*tp_itemsize*/
    (destructor) _Env_dealloc,
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,        /*tp_flags*/
    "DB Environment",          /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    _Env_methods,
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    terane_Env_new
};
