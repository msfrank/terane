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
    /* loop once a minute */
    for (;;) {
        int rejected = 0;
        /* run the deadlock detector */
        dbret = env->env->lock_detect (env->env, 0, DB_LOCK_MINLOCKS, &rejected);
        if (dbret != 0)
            log_msg (TERANE_LOG_ERROR, "terane.db.storage",
                "lock_detect failed: %s", db_strerror (dbret));
        else if (rejected > 0)
            log_msg (TERANE_LOG_DEBUG, "terane.db.storage",
                "lock_detect rejected %i requests", rejected);
        /* sleep for a minute */
        sleep (60);
        /* perform a checkpoint */
        dbret = env->env->txn_checkpoint (env->env, 0, 0, 0);
        if (dbret != 0)
            log_msg (TERANE_LOG_ERROR, "terane.db.storage", "txn_checkpoint failed: %s",
                db_strerror (dbret));
        pthread_testcancel ();
    }
    return NULL;
}

/*
 * _Env_log_err: log DB error messages s using logfd.
 */
static void
_Env_log_err (const DB_ENV *env, const char *prefix, const char *msg)
{
    log_msg (TERANE_LOG_ERROR, "terane.db.storage", "BDB: %s", msg);
}

/*
 * _Env_log_info: log DB informational messages using logfd.
 */
static void
_Env_log_msg (const DB_ENV *env, const char *msg)
{
    log_msg (TERANE_LOG_INFO, "terane.db.storage", "BDB: %s", msg);
}

/*
 * terane_Env_new: allocate a new Env object.
 *
 * callspec: Env(envdir, datadir, tmpdir, [cachesize])
 * parameters:
 *  envdir (string): A path to the DB environment
 *  datadir (string): A path to where the database data is stored
 *  tmpdir (string): A path to a directory used for temporary data
 *  cachesize (int): The size of the database cache
 * returns: A new Env object
 * exceptions:
 *  Exception: failed to create the DB_ENV handle
 */
PyObject *
terane_Env_new (PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    terane_Env *self;
    char *kwlist[] = {"envdir", "datadir", "tmpdir", "cachesize", NULL};
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
    /* set db error and message logging */
    self->env->set_errcall (self->env, _Env_log_err);
    self->env->set_msgcall (self->env, _Env_log_msg);
    self->env->set_verbose (self->env, DB_VERB_DEADLOCK, 1);
    self->env->set_verbose (self->env, DB_VERB_RECOVERY, 1);
    self->env->set_verbose (self->env, DB_VERB_REGISTER, 1);
    /* parse constructor parameters */
    if (!PyArg_ParseTupleAndKeywords (args, kwds, "sss|I",
        kwlist, &envdir, &datadir, &tmpdir, &cachesize))
        goto error;
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
    /* set db log management parameters */
    dbret = self->env->log_set_config (self->env, DB_LOG_AUTO_REMOVE, 1);
    if (dbret != 0) {
        PyErr_Format (PyExc_Exception, "Failed to enable log auto-removal: %s",
            db_strerror (dbret));
        goto error;
    }
    /* open the database environment */
    dbret = self->env->open (self->env, envdir, DB_CREATE | 
        DB_INIT_TXN | DB_INIT_MPOOL | DB_INIT_LOCK | DB_INIT_LOG |
        DB_PRIVATE | DB_THREAD | DB_RECOVER, 0);
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
