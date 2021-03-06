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

#include "backend.h"

/*
 * _Env_dealloc: free resources for the Env object.
 */
static void
_Env_dealloc (terane_Env *self)
{
    terane_Env_close (self);
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
            terane_log_msg (TERANE_LOG_ERROR, "terane.outputs.store.backend",
                "lock_detect failed: %s", db_strerror (dbret));
        else if (rejected > 0)
            terane_log_msg (TERANE_LOG_DEBUG, "terane.outputs.store.backend",
                "lock_detect rejected %i requests", rejected);
        /* sleep for a minute */
        sleep (60);
        /* perform a checkpoint */
        dbret = env->env->txn_checkpoint (env->env, 0, 0, 0);
        if (dbret != 0)
            terane_log_msg (TERANE_LOG_ERROR, "terane.outputs.store.backend", "txn_checkpoint failed: %s",
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
    terane_log_msg (TERANE_LOG_ERROR, "terane.outputs.store.backend", "BDB: %s", msg);
}

/*
 * _Env_log_info: log DB informational messages using logfd.
 */
static void
_Env_log_msg (const DB_ENV *env, const char *msg)
{
    terane_log_msg (TERANE_LOG_INFO, "terane.outputs.store.backend", "BDB: %s", msg);
}

/*
 * _Env_new: allocate a new Env object.
 *
 * callspec: Env.__new__()
 * returns: A new Env object
 * exceptions: None
 */
static PyObject *
_Env_new (PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc (type, 0);
}

/*
 * _Env_init: initialize an Env object.
 *
 * callspec: Env.__init__(envdir, datadir, tmpdir, options)
 * parameters:
 *  envdir (string): A path to the DB environment
 *  datadir (string): A path to where the database data is stored
 *  tmpdir (string): A path to a directory used for temporary data
 *  options (dict): A dict mapping string keys to option values
 * returns: 0 on success, otherwise -1
 * exceptions:
 *  Exception: failed to create the DB_ENV handle
 */
static int
_Env_init (terane_Env *self, PyObject *args, PyObject *kwds)
{
    char *envdir = NULL, *datadir = NULL, *tmpdir = NULL;
    PyObject *options = NULL, *value = NULL;
    uint32_t u32, cache_gbytes = 0, cache_bytes = 0;
    unsigned PY_LONG_LONG u64;
    int ncaches = 0, dbret;

    /* __init__ has already been called, don't repeat initialization */
    if (self->env != NULL)
        return 0;
    /* parse constructor parameters */
    if (!PyArg_ParseTuple (args, "sssO!", &envdir, &datadir, &tmpdir, &PyDict_Type, &options))
        goto error;
    /* create the DB_ENV handle */
    dbret = db_env_create (&self->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create DB_ENV: %s", db_strerror (dbret));
        goto error;
    }
    /* set db error and message logging */
    self->env->set_errcall (self->env, _Env_log_err);
    self->env->set_msgcall (self->env, _Env_log_msg);
    self->env->set_verbose (self->env, DB_VERB_DEADLOCK, 1);
    self->env->set_verbose (self->env, DB_VERB_RECOVERY, 1);
    self->env->set_verbose (self->env, DB_VERB_REGISTER, 1);
    /* set the data_dir.  datadir string should not be freed. */
    dbret = self->env->set_data_dir (self->env, datadir);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to set datadir: %s", db_strerror (dbret));
        goto error;
    }
    /* set the tmp dir.  the tmpdir string should not be freed. */
    dbret = self->env->set_tmp_dir (self->env, tmpdir);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to set tmpdir: %s", db_strerror (dbret));
        goto error;
    }
    /* set the cache size */
    if ((value = PyDict_GetItemString (options, "cache size")) && PyLong_Check (value)) {
        u64 = PyLong_AsUnsignedLongLong (value);
        cache_gbytes = u64 / (1024 * 1024 * 1024);
        cache_bytes = u64 % (1024 * 1024 * 1024);
        dbret = self->env->set_cachesize (self->env, cache_gbytes, cache_bytes, 0);
        if (dbret != 0) {
            PyErr_Format (terane_Exc_Error, "Failed to set cache size: %s", db_strerror (dbret));
            goto error;
        }
    }
    if (self->env->get_cachesize (self->env, &cache_gbytes, &cache_bytes, &ncaches) == 0) {
        u64 = (cache_gbytes * 1024 * 1024 * 1024) + cache_bytes;
        terane_log_msg (TERANE_LOG_DEBUG, "terane.outputs.store.backend",
            "environment cache is configured with %i regions, total size is %llu bytes", ncaches, u64);
    }
    /* set max txn lockers */
    if ((value = PyDict_GetItemString (options, "max lockers")) && PyLong_Check (value)) {
        u32 = (u_int32_t) PyLong_AsLong (value);
        dbret = self->env->set_lk_max_lockers (self->env, u32);
        if (dbret != 0) {
            PyErr_Format (terane_Exc_Error, "Failed to set max lockers: %s", db_strerror (dbret));
            goto error;
        }
    }
    if (self->env->get_lk_max_lockers (self->env, &u32) == 0)
        terane_log_msg (TERANE_LOG_DEBUG, "terane.outputs.store.backend",
            "environment max lockers is %i", u32);
    /* set max txn locks */
    if ((value = PyDict_GetItemString (options, "max locks")) && PyLong_Check (value)) {
        u32 = (u_int32_t) PyLong_AsLong (value);
        dbret = self->env->set_lk_max_locks (self->env, u32);
        if (dbret != 0) {
            PyErr_Format (terane_Exc_Error, "Failed to set max locks: %s", db_strerror (dbret));
            goto error;
        }
    }
    if (self->env->get_lk_max_locks (self->env, &u32) == 0)
        terane_log_msg (TERANE_LOG_DEBUG, "terane.outputs.store.backend",
            "environment max locks is %i", u32);
    /* set max txn objects */
    if ((value = PyDict_GetItemString (options, "max objects")) && PyLong_Check (value)) {
        u32 = (u_int32_t) PyLong_AsLong (value);
        dbret = self->env->set_lk_max_objects (self->env, u32);
        if (dbret != 0) {
            PyErr_Format (terane_Exc_Error, "Failed to set max objects: %s", db_strerror (dbret));
            goto error;
        }
    }
    if (self->env->get_lk_max_objects (self->env, &u32) == 0)
        terane_log_msg (TERANE_LOG_DEBUG, "terane.outputs.store.backend",
            "environment max objects is %i", u32);
    /* set max transactions */
    if ((value = PyDict_GetItemString (options, "max transactions")) && PyLong_Check (value)) 
        u32 = (u_int32_t) PyLong_AsLong (value);
    else
        u32 = 0;
    /* if u32 <= 0, then try to automatically determine the appropriate max transactions */
    if (u32 <= 0) {
        long pagesize = sysconf (_SC_PAGESIZE);
        if (pagesize < 0) {
            terane_log_msg (TERANE_LOG_ERROR, "terane.outputs.store.backend",
                "couldn't determine _SC_PAGESIZE, you need to specify 'max transactions'");
            PyErr_Format (terane_Exc_Error, "Failed to determine _SC_PAGESIZE: %s", strerror (errno));
            goto error;
        }
        /* the estimation formula is total cache size divided by page size */
        u64 = ((cache_gbytes * 1024 * 1024 * 1024) + cache_bytes) / pagesize;
        if (u64 > 0xFFFFFFFF)
            u32 = 0xFFFFFFFF;
        else
            u32 = (uint32_t) u64;
    }
    dbret = self->env->set_tx_max (self->env, u32);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to set max transactions: %s", db_strerror (dbret));
        goto error;
    }
    if (self->env->get_tx_max (self->env, &u32) == 0)
        terane_log_msg (TERANE_LOG_DEBUG, "terane.outputs.store.backend",
            "environment max transactions is %i", u32);
    /* set db log management parameters */
    dbret = self->env->log_set_config (self->env, DB_LOG_AUTO_REMOVE, 1);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to enable log auto-removal: %s",
            db_strerror (dbret));
        goto error;
    }
    /* open the database environment */
    dbret = self->env->open (self->env, envdir, DB_CREATE | 
        DB_INIT_TXN | DB_INIT_MPOOL | DB_INIT_LOCK | DB_INIT_LOG |
        DB_THREAD | DB_RECOVER, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open environment: %s",
            db_strerror (dbret));
        goto error;
    }
    /* start the checkpoint thread */
    dbret = pthread_create (&self->checkpoint_thread, NULL, _Env_checkpoint_thread, self);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to start checkpoint thread: %s",
            strerror (dbret));
    }
    else
        terane_log_msg (TERANE_LOG_DEBUG, "terane.outputs.store.backend",
            "started checkpoint thread (tid %i)", self->checkpoint_thread);

    return 0;

/* if there was an error, clean up and bail out */
error:
    if (self)
        _Env_dealloc ((terane_Env *) self);
    return -1;
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
terane_Env_close (terane_Env *self)
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
            PyErr_Format (terane_Exc_Error, "Failed to close environment: %s", db_strerror (dbret));
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
    "backend.Env",
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
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE, /*tp_flags*/
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
    (initproc) _Env_init,      /* tp_init */
    0,                         /* tp_alloc */
    _Env_new                   /* tp_new */
};
