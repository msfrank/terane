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
 * _Segment_postings_forward_compar:
 *
 */
static int
_Segment_postings_forward_compar (DB *db, const DBT *dbt1, const DBT *dbt2)
{
    int ret;

    ret = memcmp (dbt1->data, dbt2->data, dbt1->size < dbt2->size? dbt1->size : dbt2->size);
    if (ret != 0)
        return ret;
    if (dbt1->size < dbt2->size)
        return -1;
    if (dbt1->size > dbt2->size)
        return 1;
    return 0;
}

/*
 * _Segment_postings_reverse_compar:
 *
 */
static int
_Segment_postings_reverse_compar (DB *db, const DBT *dbt1, const DBT *dbt2)
{
    return 0 - _Segment_postings_forward_compar (db, dbt1, dbt2);
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
        PyErr_Format (terane_Exc_Error, "Failed to create handle for _metadata: %s",
            db_strerror (dbret));
        goto error;
    }
    /* open the metadata DB */
    dbret = self->metadata->open (self->metadata, segment_txn, self->name,
        "_metadata", DB_BTREE, DB_CREATE | DB_THREAD, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open _metadata: %s",
            db_strerror (dbret));
        goto error;
    }

    /* create the DB handle for documents */
    dbret = db_create (&self->documents, self->index->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for _documents: %s",
            db_strerror (dbret));
        goto error;
    }

    /* open the documents DB */
    dbret = self->documents->open (self->documents, segment_txn, self->name,
        "_documents", DB_BTREE, DB_CREATE | DB_THREAD, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open _documents: %s",
            db_strerror (dbret));
        goto error;
    }

    /* create the DB handle for postings */
    dbret = db_create (&self->postings, self->index->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for _postings: %s",
            db_strerror (dbret));
        goto error;
    }

    self->postings->set_bt_compare (self->postings, _Segment_postings_forward_compar);

    /* open the postings DB */
    dbret = self->postings->open (self->postings, segment_txn, self->name,
        "_postings", DB_BTREE, DB_CREATE | DB_THREAD, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open _postings: %s",
            db_strerror (dbret));
        goto error;
    }

    /* create the DB handle for reversed postings */
    dbret = db_create (&self->rpostings, self->index->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for reversed _postings: %s",
            db_strerror (dbret));
        goto error;
    }

    self->rpostings->set_bt_compare (self->rpostings, _Segment_postings_reverse_compar);

    /* open the postings DB */
    dbret = self->rpostings->open (self->rpostings, segment_txn, self->name,
        "_postings", DB_BTREE, DB_CREATE | DB_THREAD, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open reversed _postings: %s",
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
            PyErr_Format (terane_Exc_Error, "Failed to close _metadata DB: %s",
                db_strerror (dbret));
    }
    self->metadata = NULL;

    /* close the documents db */
    if (self->documents != NULL) {
        dbret = self->documents->close (self->documents, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close _documents DB: %s",
                db_strerror (dbret));
    }
    self->documents = NULL;

    /* close the postings db */
    if (self->postings != NULL) {
        dbret = self->postings->close (self->postings, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close _postings DB: %s",
                db_strerror (dbret));
    }
    self->postings = NULL;

    /* close the rpostings db */
    if (self->rpostings != NULL) {
        dbret = self->rpostings->close (self->rpostings, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close reverse _postings DB: %s",
                db_strerror (dbret));
    }
    self->rpostings = NULL;

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
    { "get_field_meta", (PyCFunction) terane_Segment_get_field_meta, METH_VARARGS,
        "Get a field metadata value." },
    { "set_field_meta", (PyCFunction) terane_Segment_set_field_meta, METH_VARARGS,
        "Set a field metadata value." },
    { "new_doc", (PyCFunction) terane_Segment_new_doc, METH_VARARGS,
        "Create a new document." },
    { "get_doc", (PyCFunction) terane_Segment_get_doc, METH_VARARGS,
        "Get a document blob by document ID." },
    { "set_doc", (PyCFunction) terane_Segment_set_doc, METH_VARARGS,
        "Set a document blob value." },
    { "delete_doc", (PyCFunction) terane_Segment_delete_doc, METH_VARARGS,
        "Delete a document blob." },
    { "contains_doc", (PyCFunction) terane_Segment_contains_doc, METH_VARARGS,
        "Returns True if the segment contains the specified document." },
    { "iter_docs", (PyCFunction) terane_Segment_iter_docs, METH_VARARGS,
        "Iterates through all documents in the segment." },
    { "get_term", (PyCFunction) terane_Segment_get_term, METH_VARARGS,
        "Get a posting in the segment inverted index." },
    { "set_term", (PyCFunction) terane_Segment_set_term, METH_VARARGS,
        "Set a posting in the segment inverted index." },
    { "contains_term", (PyCFunction) terane_Segment_contains_term, METH_VARARGS,
        "Returns True if the segment contains the specified term." },
    { "estimate_term_postings", (PyCFunction) terane_Segment_estimate_term_postings, METH_VARARGS,
        "Returns the percentage of postings in the field within the given range." },
    { "iter_terms", (PyCFunction) terane_Segment_iter_terms, METH_VARARGS,
        "Iterates through all postings in the segment." },
    { "iter_terms_within", (PyCFunction) terane_Segment_iter_terms_within, METH_VARARGS,
        "Iterates through postings in the segment between the start and end IDs." },
    { "get_term_meta", (PyCFunction) terane_Segment_get_term_meta, METH_VARARGS,
        "Get metadata for a term in the segment." },
    { "set_term_meta", (PyCFunction) terane_Segment_set_term_meta, METH_VARARGS,
        "Set metadata for a term in the segment." },
    { "iter_terms_meta", (PyCFunction) terane_Segment_iter_terms_meta, METH_VARARGS,
        "Iterates through all terms in the index." },
    { "iter_terms_meta_range", (PyCFunction) terane_Segment_iter_terms_meta_range, METH_VARARGS,
        "Iterates through all terms in the index matching the prefix." },
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
