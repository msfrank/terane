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
 * terane_Index_dealloc: free resources for the Index object.
 */
static void
_Index_dealloc (terane_Index *self)
{
    terane_Index_close (self);
    if (self->env != NULL)
        Py_DECREF (self->env);
    self->env = NULL;
    if (self->name != NULL)
        Py_DECREF (self->name);
    self->name = NULL;
    self->ob_type->tp_free ((PyObject *) self);
}

/*
 * _Index_new: allocate a new Index object.
 *
 * callspec: Index.__new__()
 * returns: A new Index object
 * exceptions: None
 */
static PyObject *
_Index_new (PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc (type, 0);
}

/*
 * _Index_init: initialize a new Index object.
 *
 * callspec: Index.__init__(env, name)
 * parameters:
 *  env (Env): A Env object to use as the environment
 *  name (string): The name of the Index
 * returns: 0 if initialization succeeded, otherwise -1
 * exceptions:
 *  terane.outputs.store.backend.Error: failed to create/open the Index
 */
static int
_Index_init (terane_Index *self, PyObject *args, PyObject *kwds)
{
    char *tocname = NULL;
    DB_TXN *txn = NULL;
    DB_BTREE_STAT *stats = NULL;
    int dbret;
    
    /* __init__ has already been called, don't repeat initialization */
    if (self->env != NULL)
        return 0;
    /* parse constructor parameters */
    if (!PyArg_ParseTuple (args, "O!O!", 
        &terane_EnvType, &self->env, &PyString_Type, &self->name))
        goto error;
    Py_INCREF (self->env);
    Py_INCREF (self->name);

    /* allocate a buffer with the full index toc name */
    tocname = PyMem_Malloc (PyString_Size (self->name) + 5);
    if (tocname == NULL) {
        PyErr_NoMemory ();
        goto error;
    }
    sprintf (tocname, "%s.toc", PyString_AsString (self->name));

    /* wrap db creation in a transaction */
    dbret = self->env->env->txn_begin (self->env->env, NULL, &txn, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create DB_TXN handle: %s",
            db_strerror (dbret));
        goto error;
    }

    /* create the DB handle for the metadata store */
    dbret = db_create (&self->metadata, self->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for metadata: %s",
            db_strerror (dbret));
        goto error;
    }
    /* set compare function */
    self->metadata->set_bt_compare (self->metadata, _terane_msgpack_DB_compare);
    /* open the metadata store */
    dbret = self->metadata->open (self->metadata, txn, tocname, "metadata",
        DB_BTREE, DB_CREATE | DB_THREAD | DB_MULTIVERSION, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open metadata: %s",
            db_strerror (dbret));
        goto error;
    }

    /* create the DB handle for the schema store */
    dbret = db_create (&self->schema, self->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for schema: %s",
            db_strerror (dbret));
        goto error;
    }
    /* set compare function */
    self->schema->set_bt_compare (self->schema, _terane_msgpack_DB_compare);
    /* open the schema store */
    dbret = self->schema->open (self->schema, txn, tocname, "schema",
        DB_BTREE, DB_CREATE | DB_THREAD | DB_MULTIVERSION, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open schema: %s",
            db_strerror (dbret));
        goto error;
    }
    /* get an initial count of fields */
    dbret = self->schema->stat (self->schema, txn, &stats, 0);
    if (dbret != 0) {
        if (stats)
            PyMem_Free (stats);
        PyErr_Format (terane_Exc_Error, "Failed to get field count: %s",
            db_strerror (dbret));
        goto error;
    }
    self->nfields = (unsigned long) stats->bt_nkeys;
    if (stats)
        PyMem_Free (stats);
    stats = NULL;

    /* create the DB handle for the segments store */
    dbret = db_create (&self->segments, self->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for segments: %s",
            db_strerror (dbret));
        goto error;
    }
    /* open the segments store */
    dbret = self->segments->open (self->segments, txn, tocname, "segments",
        DB_BTREE, DB_CREATE | DB_THREAD | DB_MULTIVERSION, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open segments: %s",
            db_strerror (dbret));
        goto error;
    }

    /* commit new databases */
    dbret = txn->commit (txn, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to commit transaction: %s",
            db_strerror (dbret));
        goto error;
    }

    PyMem_Free (tocname);

    return 0;

/* if there is an error, then free any locally allocated memory and references */
error:
    if (txn != NULL)
        txn->abort (txn);
    if (tocname != NULL)
        PyMem_Free (tocname);
    if (self)
        _Index_dealloc ((terane_Index *) self);
    return -1;
}

/*
 * terane_Index_new_txn: create a new top-level Txn object.
 *
 * callspec: Index.new_txn(READ_COMMITTED=False, READ_UNCOMMITTED=False, TXN_NOSYNC=False,
    TXN_NOWAIT=False, TXN_SNAPSHOT=False, TXN_WAIT_NOSYNC=False)
 * parameters:
 *  READ_COMMITTED (bool):
 *  READ_UNCOMMITTED (bool):
 *  TXN_NOSYNC (bool):
 *  TXN_NOWAIT (bool):
 *  TXN_SNAPSHOT (bool):
 *  TXN_WAIT_NOSYNC (bool):
 * returns: A new Txn object.
 * exceptions:
 *  terane.outputs.store.backend:Error: failed to create a DB_TXN handle.
 */
PyObject *
terane_Index_new_txn (terane_Index *self, PyObject *args, PyObject *kwds)
{
    return terane_Txn_new (self->env, NULL, args, kwds);
}

/*
 * terane_Index_close: close the underlying DB handles.
 *
 * callspec: Index.close()
 * parameters: None
 * returns: None
 * exceptions:
 *  terane.outputs.store.backend.Error: failed to close a db in the Index
 */
PyObject *
terane_Index_close (terane_Index *self)
{
    int dbret;

    /* close the metadata db */
    if (self->metadata != NULL) {
        dbret = self->metadata->close (self->metadata, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close metadata: %s",
                db_strerror (dbret));
    }
    self->metadata = NULL;

    /* close the schema db */
    if (self->schema != NULL) {
        dbret = self->schema->close (self->schema, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close schema: %s",
                db_strerror (dbret));
    }
    self->schema = NULL;

    /* close the segments db */
    if (self->segments != NULL) {
        dbret = self->segments->close (self->segments, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close segments: %s",
                db_strerror (dbret));
    }
    self->segments = NULL;

    Py_RETURN_NONE;
}

/* Index methods declaration */
PyMethodDef _Index_methods[] =
{
    { "get_meta", (PyCFunction) terane_Index_get_meta, METH_VARARGS,
        "Get a Index metadata value." },
    { "set_meta", (PyCFunction) terane_Index_set_meta, METH_VARARGS,
        "Set a Index metadata value." },
    { "get_field", (PyCFunction) terane_Index_get_field, METH_VARARGS,
        "Get a field in the Index." },
    { "add_field", (PyCFunction) terane_Index_add_field, METH_VARARGS,
        "Add a field to the Index." },
    { "contains_field", (PyCFunction) terane_Index_contains_field, METH_VARARGS,
        "Return True if the field exists in the Index." },
    { "iter_fields", (PyCFunction) terane_Index_iter_fields, METH_VARARGS,
        "Iterate through all fields in the Index." },
    { "count_fields", (PyCFunction) terane_Index_count_fields, METH_NOARGS,
        "Return the count of fields in the Index." },
    { "add_segment", (PyCFunction) terane_Index_add_segment, METH_VARARGS,
        "Add a new segment." },
    { "iter_segments", (PyCFunction) terane_Index_iter_segments, METH_VARARGS,
        "Iterate all segments in the Index." },
    { "delete_segment", (PyCFunction) terane_Index_delete_segment, METH_VARARGS,
        "Delete the segment from the Index." },
    { "new_txn", (PyCFunction) terane_Index_new_txn, METH_VARARGS|METH_KEYWORDS,
        "Create a top-level Txn." },
    { "close", (PyCFunction) terane_Index_close, METH_NOARGS,
        "Close the Index." },
    { NULL, NULL, 0, NULL }
};

/* Index type declaration */
PyTypeObject terane_IndexType = {
    PyObject_HEAD_INIT(NULL)
    0,
    "backend.Index",
    sizeof (terane_Index),
    0,                         /*tp_itemsize*/
    (destructor) _Index_dealloc,
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
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,
    "DB Index",                /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    _Index_methods,
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc) _Index_init,    /* tp_init */
    0,                         /* tp_alloc */
    _Index_new
};
