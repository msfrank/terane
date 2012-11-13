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
 * _Segment_dealloc: free resources for the Segment object.
 */
static void
_Segment_dealloc (terane_Segment *self)
{
    terane_Segment_close (self);
    if (self->index != NULL)
        Py_DECREF (self->index);
    self->index = NULL;
    if (self->name != NULL)
        PyMem_Free (self->name);
    self->name = NULL;
    self->ob_type->tp_free ((PyObject *) self);
}

/*
 * _Segment_new: allocate a new Segment object.
 *
 * callspec: Segment.__new__()
 * returns: A new Segment object
 * exceptions: None
 */
static PyObject *
_Segment_new (PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return type->tp_alloc (type, 0);
}

/*
 * _Segment_init: initialize a Segment object.
 *
 * callspec: Segment.__init__(txn, index, sid)
 * parameters:
 *  txn (Txn): A Txn object to wrap the operation in
 *  index (Index): An Index object to use for bookkeeping
 *  sid (long): The segment id
 * returns: 0 on success, otherwise -1
 * exceptions:
 *  terane.outputs.store.backend.Error: failed to create/open the Segment
 */
static int
_Segment_init (terane_Segment *self, PyObject *args, PyObject *kwds)
{
    terane_Txn *txn = NULL;
    db_recno_t segment_id = 0;
    DB_TXN *segment_txn = NULL;
    int exists, dbret;

    /* __init__ has already been called, don't repeat initialization */
    if (self->index != NULL)
        return 0;
    /* parse constructor parameters */
    if (!PyArg_ParseTuple (args, "O!O!k",
        &terane_TxnType, &txn, &terane_IndexType, &self->index, &segment_id))
        goto error;
    Py_INCREF (self->index);

    /* verify the segment exists in the TOC */
    exists = terane_Index_contains_segment (self->index, txn, segment_id);
    if (exists < 0)
        goto error;
    if (exists == 0) {
        PyErr_Format (PyExc_KeyError, "Segment %lu doesn't exist",
            (unsigned long int) segment_id);
        goto error;
    }
    /* allocate a buffer large enough to hold the longest segment name */
    self->name = PyMem_Malloc (PyString_Size (self->index->name) + 12);
    if (self->name == NULL) {
        PyErr_NoMemory ();
        goto error;
    }
    sprintf (self->name, "%s.%lu", PyString_AsString (self->index->name),
        (unsigned long int) segment_id);

    /* wrap db creation in a transaction */
    dbret = self->index->env->env->txn_begin (self->index->env->env, txn->txn, &segment_txn, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create transaction: %s",
            db_strerror (dbret));
        goto error;
    }

    /* create the DB handle for metadata */
    dbret = db_create (&self->metadata, self->index->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for metadata: %s",
            db_strerror (dbret));
        goto error;
    }
    /* set compare function */
    self->metadata->set_bt_compare (self->metadata, _terane_msgpack_DB_compare);
    /* open the metadata DB */
    dbret = self->metadata->open (self->metadata, segment_txn, self->name,
        "metadata", DB_BTREE, DB_CREATE | DB_THREAD | DB_MULTIVERSION, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open metadata: %s",
            db_strerror (dbret));
        goto error;
    }

    /* create the DB handle for events */
    dbret = db_create (&self->events, self->index->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for events: %s",
            db_strerror (dbret));
        goto error;
    }
    /* set compare function */
    self->events->set_bt_compare (self->events, _terane_msgpack_DB_compare);
    /* open the events DB */
    dbret = self->events->open (self->events, segment_txn, self->name,
        "events", DB_BTREE, DB_CREATE | DB_THREAD | DB_MULTIVERSION, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open events: %s",
            db_strerror (dbret));
        goto error;
    }

    /* create the DB handle for postings */
    dbret = db_create (&self->postings, self->index->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for postings: %s",
            db_strerror (dbret));
        goto error;
    }
    /* set compare function */
    self->postings->set_bt_compare (self->postings, _terane_msgpack_DB_compare);
    /* open the postings DB */
    dbret = self->postings->open (self->postings, segment_txn, self->name,
        "postings", DB_BTREE, DB_CREATE | DB_THREAD | DB_MULTIVERSION, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open postings: %s",
            db_strerror (dbret));
        goto error;
    }

    /* create the DB handle for fields */
    dbret = db_create (&self->fields, self->index->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for fields: %s",
            db_strerror (dbret));
        goto error;
    }
    /* set compare function */
    self->fields->set_bt_compare (self->fields, _terane_msgpack_DB_compare);
    /* open the fields DB */
    dbret = self->fields->open (self->fields, segment_txn, self->name,
        "fields", DB_BTREE, DB_CREATE | DB_THREAD | DB_MULTIVERSION, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open fields: %s",
            db_strerror (dbret));
        goto error;
    }

    /* create the DB handle for terms */
    dbret = db_create (&self->terms, self->index->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for terms: %s",
            db_strerror (dbret));
        goto error;
    }
    /* set compare function */
    self->terms->set_bt_compare (self->terms, _terane_msgpack_DB_compare);
    /* open the terms DB */
    dbret = self->terms->open (self->terms, segment_txn, self->name,
        "terms", DB_BTREE, DB_CREATE | DB_THREAD | DB_MULTIVERSION, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open terms: %s",
            db_strerror (dbret));
        goto error;
    }

    /* commit new databases */
    dbret = segment_txn->commit (segment_txn, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to commit transaction: %s",
            db_strerror (dbret));
        txn = NULL;
        goto error;
    }

    /* return the initialized Segment object on success */
    return 0;

/* if there is an error, then free any locally allocated memory and references */
error:
    if (segment_txn != NULL)
        segment_txn->abort (segment_txn);
    if (self)
        _Segment_dealloc ((terane_Segment *) self);
    return -1;
}

/*
 * terane_Segment_delete: Mark the Segment for deletion.
 */
PyObject *
terane_Segment_delete (terane_Segment *self)
{
    self->deleted = 1;
    Py_RETURN_NONE;
}

/*
 * terane_Segment_close: close the underlying DB handles.
 *
 * callspec: Segment.close()
 * parameters: None
 * returns: None
 * exceptions:
 *  terane.outputs.store.backend.Error: failed to close a db in the Segment
 */
PyObject *
terane_Segment_close (terane_Segment *self)
{
    int dbret;

    /* close the metadata db */
    if (self->metadata != NULL) {
        dbret = self->metadata->close (self->metadata, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close metadata DB: %s",
                db_strerror (dbret));
    }
    self->metadata = NULL;

    /* close the events db */
    if (self->events != NULL) {
        dbret = self->events->close (self->events, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close events DB: %s",
                db_strerror (dbret));
    }
    self->events = NULL;

    /* close the postings db */
    if (self->postings != NULL) {
        dbret = self->postings->close (self->postings, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close postings DB: %s",
                db_strerror (dbret));
    }
    self->postings = NULL;

    /* close the fields db */
    if (self->fields != NULL) {
        dbret = self->fields->close (self->fields, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close fields DB: %s",
                db_strerror (dbret));
    }
    self->fields = NULL;

    /* close the terms db */
    if (self->terms != NULL) {
        dbret = self->terms->close (self->terms, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close terms DB: %s",
                db_strerror (dbret));
    }
    self->terms = NULL;

    /* if this segment is marked to be deleted */
    if (self->deleted) {
        dbret = self->index->env->env->dbremove (self->index->env->env, NULL,
            self->name, NULL, DB_AUTO_COMMIT);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to delete segment: %s",
                db_strerror (dbret));
    }

    Py_RETURN_NONE;
}

/* Segment methods declaration */
PyMethodDef _Segment_methods[] =
{
    { "get_meta", (PyCFunction) terane_Segment_get_meta, METH_VARARGS,
        "Get a Segment metadata value." },
    { "set_meta", (PyCFunction) terane_Segment_set_meta, METH_VARARGS,
        "Set a Segment metadata value." },
    { "get_field", (PyCFunction) terane_Segment_get_field, METH_VARARGS,
        "Get a field metadata value." },
    { "set_field", (PyCFunction) terane_Segment_set_field, METH_VARARGS,
        "Set a field metadata value." },
    { "new_event", (PyCFunction) terane_Segment_new_event, METH_VARARGS,
        "Create a new event." },
    { "get_event", (PyCFunction) terane_Segment_get_event, METH_VARARGS,
        "Get an event blob by event identifier." },
    { "set_event", (PyCFunction) terane_Segment_set_event, METH_VARARGS,
        "Set an event value." },
    { "delete_event", (PyCFunction) terane_Segment_delete_event, METH_VARARGS,
        "Delete an event." },
    { "contains_event", (PyCFunction) terane_Segment_contains_event, METH_VARARGS,
        "Returns True if the segment contains the specified event." },
    { "estimate_events", (PyCFunction) terane_Segment_estimate_events, METH_VARARGS,
        "Returns the percentage of events within the given range." },
    { "iter_events", (PyCFunction) terane_Segment_iter_events, METH_VARARGS,
        "Iterates through all event identifers in the segment between the start and end event identifier." },
    { "get_term", (PyCFunction) terane_Segment_get_term, METH_VARARGS,
        "Get metadata for a term in the segment." },
    { "set_term", (PyCFunction) terane_Segment_set_term, METH_VARARGS,
        "Set metadata for a term in the segment." },
    { "get_posting", (PyCFunction) terane_Segment_get_posting, METH_VARARGS,
        "Get a posting in the segment inverted index." },
    { "contains_posting", (PyCFunction) terane_Segment_contains_posting, METH_VARARGS,
        "Returns True if the segment contains the specified posting." },
    { "set_posting", (PyCFunction) terane_Segment_set_posting, METH_VARARGS,
        "Set a posting in the segment inverted index." },
    { "estimate_postings", (PyCFunction) terane_Segment_estimate_postings, METH_VARARGS,
        "Returns the percentage of postings in the field within the given range." },
    { "iter_postings", (PyCFunction) terane_Segment_iter_postings, METH_VARARGS,
        "Iterates through all postings in the segment." },
    { "delete", (PyCFunction) terane_Segment_delete, METH_NOARGS,
        "Mark the DB Segment for deletion.  Actual deletion will not occur until the Segment is deallocated." },
    { "close", (PyCFunction) terane_Segment_close, METH_NOARGS,
        "Close the DB Segment." },
    { NULL, NULL, 0, NULL }
};

/* Segment type declaration */
PyTypeObject terane_SegmentType = {
    PyObject_HEAD_INIT(NULL)
    0,
    "backend.Segment",
    sizeof (terane_Segment),
    0,                         /*tp_itemsize*/
    (destructor) _Segment_dealloc,
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
    "DB Segment",                /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    _Segment_methods,
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc) _Segment_init,  /* tp_init */
    0,                         /* tp_alloc */
    _Segment_new               /* tp_new */
};
