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
 * terane_TOC_dealloc: free resources for the TOC object.
 */
static void
_TOC_dealloc (terane_TOC *self)
{
    terane_TOC_close (self);
    if (self->env != NULL)
        Py_DECREF (self->env);
    self->env = NULL;
    if (self->name != NULL)
        Py_DECREF (self->name);
    self->name = NULL;
    self->ob_type->tp_free ((PyObject *) self);
}

/*
 * terane_TOC_new: allocate a new TOC object.
 *
 * callspec: TOC(env, name)
 * parameters:
 *  env (Env): A Env object to use as the environment
 *  name (string): The name of the Index
 * returns: A new TOC object
 * exceptions:
 *  terane.outputs.store.backend.Error: failed to create/open the TOC
 */
PyObject *
terane_TOC_new (PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    terane_TOC *self;
    char *kwlist[] = {"env", "name", NULL};
    char *tocname = NULL;
    DB_TXN *txn = NULL;
    DB_BTREE_STAT *stats = NULL;
    int dbret;

    /* allocate the TOC object */
    self = (terane_TOC *) type->tp_alloc (type, 0);
    if (self == NULL) {
        PyErr_SetString (terane_Exc_Error, "Failed to allocate TOC");
        return NULL;
    }
    self->name = NULL;
    self->metadata = NULL;
    self->schema = NULL;
    self->segments = NULL;

    /* parse constructor parameters */
    if (!PyArg_ParseTupleAndKeywords (args, kwds, "O!O!", kwlist,
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
        PyErr_Format (terane_Exc_Error, "Failed to create handle for _metadata: %s",
            db_strerror (dbret));
        goto error;
    }
    /* open the metadata store */
    dbret = self->metadata->open (self->metadata, txn, tocname, "_metadata",
        DB_BTREE, DB_CREATE | DB_THREAD, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open _metadata: %s",
            db_strerror (dbret));
        goto error;
    }

    /* create the DB handle for the schema store */
    dbret = db_create (&self->schema, self->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for _schema: %s",
            db_strerror (dbret));
        goto error;
    }
    /* open the schema store */
    dbret = self->schema->open (self->schema, txn, tocname, "_schema",
        DB_BTREE, DB_CREATE | DB_THREAD, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open _schema: %s",
            db_strerror (dbret));
        goto error;
    }

    /* get an initial count of fields.  we don't wrap this call in
     * a transaction because we aren't making any modifications, and there
     * is no possibility of external modification during this call.
     */
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
        PyErr_Format (terane_Exc_Error, "Failed to create handle for _segments: %s",
            db_strerror (dbret));
        goto error;
    }
    /* open the segments store */
    dbret = self->segments->open (self->segments, txn, tocname, "_segments",
        DB_RECNO, DB_CREATE | DB_THREAD, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open _segments: %s",
            db_strerror (dbret));
        goto error;
    }

    /* get an initial count of segments.  we don't wrap this call in
     * a transaction because we aren't making any modifications, and there
     * is no possibility of external modification during this call.
     */
    dbret = self->segments->stat (self->segments, txn, &stats, 0);
    if (dbret != 0) {
        if (stats)
            PyMem_Free (stats);
        PyErr_Format (terane_Exc_Error, "Failed to get segment count: %s",
            db_strerror (dbret));
        goto error;
    }
    self->nsegments = (unsigned long) stats->bt_nkeys;
    if (stats)
        PyMem_Free (stats);
    stats = NULL;
  
    /* commit new databases */
    dbret = txn->commit (txn, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to commit transaction: %s",
            db_strerror (dbret));
        goto error;
    }

    PyMem_Free (tocname);

    /* return the initialized TOC object on success */
    return (PyObject *) self;

/* if there is an error, then free any locally allocated memory and references */
error:
    if (txn != NULL)
        txn->abort (txn);
    if (tocname != NULL)
        PyMem_Free (tocname);
    if (self)
        _TOC_dealloc ((terane_TOC *) self);
    return NULL;
}

/*
 * terane_TOC_new_txn: create a new top-level Txn object.
 *
 * callspec: TOC.new_txn()
 * parameters: None
 * returns: A new Txn object.
 * exceptions:
 *  terane.outputs.store.backend:Error: failed to create a DB_TXN handle.
 */
PyObject *
terane_TOC_new_txn (terane_TOC *self)
{
    return terane_Txn_new (self->env, NULL);
}

/*
 * terane_TOC_close: close the underlying DB handles.
 *
 * callspec: TOC.close()
 * parameters: None
 * returns: None
 * exceptions:
 *  terane.outputs.store.backend.Error: failed to close a db in the TOC
 */
PyObject *
terane_TOC_close (terane_TOC *self)
{
    int dbret;

    /* close the metadata db */
    if (self->metadata != NULL) {
        dbret = self->metadata->close (self->metadata, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close _metadata: %s",
                db_strerror (dbret));
    }
    self->metadata = NULL;

    /* close the schema db */
    if (self->schema != NULL) {
        dbret = self->schema->close (self->schema, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close _schema: %s",
                db_strerror (dbret));
    }
    self->schema = NULL;

    /* close the segments db */
    if (self->segments != NULL) {
        dbret = self->segments->close (self->segments, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close _segments: %s",
                db_strerror (dbret));
    }
    self->segments = NULL;
    Py_RETURN_NONE;
}

/* TOC methods declaration */
PyMethodDef _TOC_methods[] =
{
    { "get_meta", (PyCFunction) terane_TOC_get_meta, METH_VARARGS,
        "Get a TOC metadata value." },
    { "set_meta", (PyCFunction) terane_TOC_set_meta, METH_VARARGS,
        "Set a TOC metadata value." },
    { "get_field", (PyCFunction) terane_TOC_get_field, METH_VARARGS,
        "Get a field in the TOC." },
    { "add_field", (PyCFunction) terane_TOC_add_field, METH_VARARGS,
        "Add a field to the TOC." },
    { "remove_field", (PyCFunction) terane_TOC_remove_field, METH_VARARGS,
        "Remove a field from the TOC." },
    { "contains_field", (PyCFunction) terane_TOC_contains_field, METH_VARARGS,
        "Return True if the field exists in the TOC." },
    { "list_fields", (PyCFunction) terane_TOC_list_fields, METH_VARARGS,
        "Return a list of all fields in the TOC." },
    { "count_fields", (PyCFunction) terane_TOC_count_fields, METH_NOARGS,
        "Return the count of fields in the TOC." },
    { "new_segment", (PyCFunction) terane_TOC_new_segment, METH_VARARGS,
        "Allocate a new segment ID." },
    { "iter_segments", (PyCFunction) terane_TOC_iter_segments, METH_VARARGS,
        "Iterate all segment IDs in the TOC." },
    { "count_segments", (PyCFunction) terane_TOC_count_segments, METH_NOARGS,
        "Return the count of segments in the TOC." },
    { "delete_segment", (PyCFunction) terane_TOC_delete_segment, METH_VARARGS,
        "Delete the segment from the TOC." },
    { "new_txn", (PyCFunction) terane_TOC_new_txn, METH_NOARGS,
        "Create a top-level Txn." },
    { "close", (PyCFunction) terane_TOC_close, METH_NOARGS,
        "Close the TOC." },
    { NULL, NULL, 0, NULL }
};

/* TOC type declaration */
PyTypeObject terane_TOCType = {
    PyObject_HEAD_INIT(NULL)
    0,
    "backend.TOC",
    sizeof (terane_TOC),
    0,                         /*tp_itemsize*/
    (destructor) _TOC_dealloc,
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
    "DB TOC",                  /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    _TOC_methods,
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    terane_TOC_new
};
