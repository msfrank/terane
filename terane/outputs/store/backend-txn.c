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
 * _Txn_dealloc: free resources for the Txn object.
 */
static void
_Txn_dealloc (terane_Txn *self)
{
    if (self->txn != NULL)
        terane_Txn_abort (self);
    if (self->env)
        Py_DECREF (self->env);
    self->ob_type->tp_free ((PyObject *) self);
}

/*
 * terane_Txn_new: allocate a new Txn object.
 *
 * parameters:
 *  env (terane.outputs.store.backend:Env): The database environment
 *  parent (terane.outputs.store.backend:Txn): A parent Txn, or NULL
 *  args: 
 *  kwds:
 * returns: A new terane.outputs.store.backend:Txn object
 * exceptions:
 *  terane.outputs.store.backend:Error: failed to create the DB_TXN handle
 */
PyObject *
terane_Txn_new (terane_Env *env, terane_Txn *parent, PyObject *args, PyObject *kwds)
{
    terane_Txn *txn, *prev;
    int dbret, dbflags = 0;
    PyObject *read_committed = NULL;
    PyObject *read_uncommitted = NULL;
    PyObject *txn_nosync = NULL;
    PyObject *txn_nowait = NULL;
    PyObject *txn_snapshot = NULL;
    PyObject *txn_write_nosync = NULL;

    char *kwdlist[] = {
        "READ_COMMITTED",
        "READ_UNCOMMITTED",
        "TXN_NOSYNC",
        "TXN_NOWAIT",
        "TXN_SNAPSHOT", 
        "TXN_WRITE_NOSYNC",
        NULL
    };

    assert (env != NULL);

    /* parse parameters and set transaction flags */
    if (!PyArg_ParseTupleAndKeywords (args, kwds, "|OOOOOO", kwdlist, &read_committed,
      &read_uncommitted, &txn_nosync, &txn_nowait, &txn_snapshot, &txn_write_nosync))
        return NULL;
    if (read_committed && PyObject_IsTrue (read_committed))
        dbflags |= DB_READ_COMMITTED;
    if (read_uncommitted && PyObject_IsTrue (read_uncommitted))
        dbflags |= DB_READ_UNCOMMITTED;
    if (txn_nosync && PyObject_IsTrue (txn_nosync))
        dbflags |= DB_TXN_NOSYNC;
    if (txn_nowait && PyObject_IsTrue (txn_nowait))
        dbflags |= DB_TXN_NOWAIT;
    if (txn_snapshot && PyObject_IsTrue (txn_snapshot))
        dbflags |= DB_TXN_SNAPSHOT;
    if (txn_write_nosync && PyObject_IsTrue (txn_write_nosync))
        dbflags |= DB_TXN_WRITE_NOSYNC;

    /* allocate the Txn object */
    txn = PyObject_New (terane_Txn, &terane_TxnType);
    if (txn == NULL)
        return NULL;
    /* add a reference to the Env object */
    Py_INCREF (env);
    txn->env = env;
    /* create the DB_TXN handle */
    txn->txn = NULL;
    dbret = env->env->txn_begin (env->env, parent ? parent->txn : NULL, &txn->txn, dbflags);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create DB_TXN: %s", db_strerror (dbret));
        goto error;
    }
    /* if there is a parent Txn, then add self to parent's list of children */
    txn->children = NULL;
    txn->next = NULL;
    if (parent != NULL) {
        Py_INCREF (txn);
        if (parent->children == NULL)
            parent->children = txn;
        else {
            prev = parent->children;
            while (prev->next)
                prev = prev->next;
            prev->next = txn;
        }
    }
    return (PyObject *) txn;

/* if there was an error, clean up and bail out */
error:
    _Txn_dealloc ((terane_Txn *) txn);
    return NULL;
}

/*
 * terane_Txn_new_txn: Create a child Txn.
 *
 * parameters:
 * returns: A new terane.outputs.store.backend:Txn object
 * exceptions:
 *  terane.outputs.store.backend:Error: failed to create a DB_TXN handle.
 */
PyObject *
terane_Txn_new_txn (terane_Txn *self, PyObject *args, PyObject *kwds)
{
    return terane_Txn_new (self->env, self, args, kwds);
}

/*
 * _Txn_discard: discard the invalid Txn handle, and all child handles.
 */
static void
_Txn_discard (terane_Txn *txn)
{
    terane_Txn *curr, *next;

    if (txn == NULL)
        return;
    txn->txn = NULL;
    curr = txn->children;
    while (curr != NULL) {
        next = curr->next;
        curr->next = NULL;
        _Txn_discard (curr);
        Py_DECREF (curr);
        curr = next;
    }
    txn->children = NULL;
}

/*
 * terane_Txn_commit: commit the transaction operations.
 *
 * callspec: Txn.commit()
 * parameters: None
 * returns: None
 * exceptions:
 *  terane.outputs.store.backend:Deadlock: deadlock was detected.
 *  terane.outputs.store.backend:LockTimeout: timed out trying to grab the lock.
 *  terane.outputs.store.backend:Error: the commit failed.
 */
PyObject *
terane_Txn_commit (terane_Txn *self)
{
    int dbret;

    if (self->txn == NULL)
        return PyErr_Format (terane_Exc_Error, "Failed to commit transaction: DB_TXN handle is NULL");
    /* try to commit the transaction */
    dbret = self->txn->commit (self->txn, 0);
    /* 
     * regardless of return status, the DB_TXN handle is invalid now, as are all child
     * transactions, so discard all references to them.
     */
    _Txn_discard (self);
    /* if dbret is not 0, then set exception and return -1 indicating failure */
    switch (dbret) {
        case 0:
            break;
        case DB_LOCK_DEADLOCK:
            PyErr_Format (terane_Exc_Deadlock, "Failed to commit transaction: %s", db_strerror (dbret));
            break;
        case DB_LOCK_NOTGRANTED:
            PyErr_Format (terane_Exc_LockTimeout, "Failed to commit transaction: %s", db_strerror (dbret));
            break;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to commit transaction: %s", db_strerror (dbret));
            break;
    }
    Py_RETURN_NONE;
}

/*
 * terane_Txn_abort: abort the transaction operations.
 *
 * callspec: Txn.abort()
 * parameters: None
 * returns: None
 * exceptions:
 *  terane.outputs.store.backend:Error: the abort failed.
 */
PyObject *
terane_Txn_abort (terane_Txn *self)
{
    int dbret;

    if (self->txn == NULL)
        return PyErr_Format (terane_Exc_Error, "Failed to abort transaction: DB_TXN handle is NULL");
    /* abort the transaction */
    dbret = self->txn->abort (self->txn);
    /* 
     * regardless of return status, the DB_TXN handle is invalid now, as are all child
     * transactions, so discard all references to them.
     */
    _Txn_discard (self);
    /* if dbret is not 0, then set exception and return -1 indicating failure */
    switch (dbret) {
        case 0:
            break;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to abort transaction: %s", db_strerror (dbret));
            break;
    }
    Py_RETURN_NONE;
}

/*
 * terane_Txn_enter: Enter the transaction context.
 *
 * callspec: Txn.__enter__()
 * parameters: None
 * returns: The Txn instance.
 * exceptions:
 *  terane.outputs.store.backend:Error: the Txn instance is invalid.
 */
PyObject *
terane_Txn_enter (terane_Txn *self)
{
    if (self->txn == NULL) {
        PyErr_Format (terane_Exc_Error, "Failed to enter transaction context: DB_TXN handle is NULL");
        Py_RETURN_NONE;
    }
    Py_INCREF (self);
    return (PyObject *) self;
}

/*
 * terane_Txn_exit: Leave the transaction context.
 *
 * callspec: Txn.__exit__(type, value, tb)
 * parameters:
 *   type: The exception class
 *   value: The exception instance
 *   tb: The exception traceback
 * returns: False
 * exceptions:
 *  terane.outputs.store.backend:Deadlock: deadlock was detected.
 *  terane.outputs.store.backend:LockTimeout: timed out trying to grab the lock.
 *  terane.outputs.store.backend:Error: the commit or abort failed.
 */
PyObject *
terane_Txn_exit (terane_Txn *self, PyObject *args)
{
    PyObject *type = NULL, *value = NULL, *tb = NULL;

    if (!PyArg_ParseTuple (args, "OOO", &type, &value, &tb))
        return NULL;
    /* if all parameters are None, then the operations succeeded */
    if (type == Py_None && value == Py_None && tb == Py_None)
        terane_Txn_commit (self);
    else
        terane_Txn_abort (self);
    Py_RETURN_FALSE;
}

/* Txn methods declaration */
PyMethodDef _Txn_methods[] =
{
    { "new_txn", (PyCFunction) terane_Txn_new_txn, METH_VARARGS|METH_KEYWORDS, "Create a child Txn." },
    { "commit", (PyCFunction) terane_Txn_commit, METH_NOARGS, "Close the DB Txn." },
    { "abort", (PyCFunction) terane_Txn_abort, METH_NOARGS, "Close the DB Txn." },
    { "__enter__", (PyCFunction) terane_Txn_enter, METH_NOARGS, "Enter the DB Txn context." },
    { "__exit__", (PyCFunction) terane_Txn_exit, METH_VARARGS, "Exit the DB Txn context." },
    { NULL, NULL, 0, NULL }
};

/* Txn type declaration */
PyTypeObject terane_TxnType = {
    PyObject_HEAD_INIT(NULL)
    0,
    "backend.Txn",
    sizeof (terane_Txn),
    0,                         /*tp_itemsize*/
    (destructor) _Txn_dealloc,
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
    "DB Transaction",          /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    _Txn_methods,
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    0,                         /* tp_new */
};
