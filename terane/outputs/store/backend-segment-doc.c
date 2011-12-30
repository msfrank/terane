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
 * terane_Segment_new_doc: create a new document
 *
 * callspec: Segment.new_doc(txn, docId)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   docId (str): The document ID string
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.DocExists: The specified document id already exists
 *   terane.outputs.store.backend.Error: A db error occurred when trying to create the record
 */
PyObject *
terane_Segment_new_doc (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    char *doc_id = NULL;
    DBT key, data;
    int id_len = 0, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!s#", &terane_TxnType, &txn, &doc_id, &id_len))
        return NULL;
    /* use the document id as the key */
    memset (&key, 0, sizeof (DBT));
    memset (&data, 0, sizeof (DBT));
    key.data = doc_id;
    key.size = id_len + 1;
    /* put a new document.  raise DocExists if the document ID already exists. */
    dbret = self->documents->put (self->documents, txn->txn, &key, &data, DB_NOOVERWRITE);
    switch (dbret) {
        case 0:
            break;
        case DB_KEYEXIST:
            return PyErr_Format (terane_Exc_DocExists,
                "Document id %s already exists", doc_id);
        default:
            return PyErr_Format (terane_Exc_Error, "Failed to create document: %s",
                db_strerror (dbret));
    }
    Py_RETURN_NONE;
}

/*
 * terane_Segment_get_doc: retrieve a document
 *
 * callspec: Segment.get_doc(txn, docId)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   docId (str): The document ID string
 * returns: A string representing the document contents 
 * exceptions:
 *   KeyError: The document with the specified id doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to retrieve the record
 */
PyObject *
terane_Segment_get_doc (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    char *doc_id = NULL;
    DBT key, data;
    PyObject *document = NULL;
    int id_len = 0, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "Os#", &txn, &doc_id, &id_len))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    /* use the document id as the key */
    memset (&key, 0, sizeof (DBT));
    key.data = doc_id;
    key.size = id_len + 1;
    /* get the document */
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    dbret = self->documents->get (self->documents, txn? txn->txn : NULL, &key, &data, 0);
    switch (dbret) {
        case 0:
            /* create a python string from the data */
            document = PyString_FromString ((char *) data.data);
            break;
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            /* document doesn't exist, raise KeyError */
            PyErr_Format (PyExc_KeyError, "Document id %s doesn't exist",
                (char *) key.data);
            break;
        default:
            /* some other db error, raise Error */
            PyErr_Format (terane_Exc_Error, "Failed to get document %s: %s",
                (char *) key.data, db_strerror (dbret));
            break;
    }
    /* free allocated memory */
    if (data.data)
        PyMem_Free (data.data);
    return document;
}

/*
 * terane_Segment_set_doc: set the document contents
 *
 * callspec: Segment.set_doc(txn, docId, document)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   docId (str): The document ID string
 *   document (str): Data to store in the document
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_set_doc (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    char *doc_id = NULL;
    const char *document = NULL;
    DBT key, data;
    int id_len = 0, doc_len = 0, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!s#s#", &terane_TxnType, &txn, &doc_id, &id_len, &document, &doc_len))
        return NULL;
    /* use the document id as the record number */
    memset (&key, 0, sizeof (DBT));
    key.data = doc_id;
    key.size = id_len;
    /* set the document from the data parameter */
    memset (&data, 0, sizeof (DBT));
    data.data = document;
    data.size = doc_len + 1;
    /* set the record */
    dbret = self->documents->put (self->documents, txn->txn, &key, &data, 0);
    /* db error, raise Exception */
    switch (dbret) {
        case 0:
            break;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to set document %s: %s",
                (char *) key.data, db_strerror (dbret));
            break;
    }
    Py_RETURN_NONE;
}

/*
 * terane_Segment_delete_doc: Delete a document record.
 *
 * callspec: Segment.delete_doc(txn, docId)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   docId (str): The document ID string
 * returns: None
 * exceptions:
 *   KeyError: The document with the specified id doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to delete the record
 */
PyObject *
terane_Segment_delete_doc (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    char *doc_id = NULL;
    DBT key;
    int id_len = 0, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!s#", &terane_TxnType, &txn, &doc_id, &id_len))
        return NULL;
    /* get the document id */
    memset (&key, 0, sizeof (DBT));
    key.data = doc_id;
    key.size = id_len + 1;
    /* delete the record */
    dbret = self->documents->del (self->documents, txn->txn, &key, 0);
    switch (dbret) {
        case 0:
            break;
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            /* document doesn't exist, raise KeyError */
            PyErr_Format (PyExc_KeyError, "Document id %s doesn't exist", (char *) key.data);
            break;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to delete document %s: %s",
                (char *) key.data, db_strerror (dbret));
            break;
    }
    Py_RETURN_NONE;
}

/*
 * terane_Segment_contains_doc: Determine whether a document exists.
 *
 * callspec: Segment.contains_doc(txn, docId)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   docId (str): The document ID string
 * returns: True if the document exists, otherwise False.
 * exceptions:
 *   Exception: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_contains_doc (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    char *doc_id = NULL;
    DBT key;
    int id_len = 0, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "Os#", &txn, &doc_id, &id_len))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    /* get the document id */
    memset (&key, 0, sizeof (DBT));
    key.data = doc_id;
    key.size = id_len + 1;
    /* check for the record */
    dbret = self->documents->exists (self->documents, txn? txn->txn : NULL, &key, 0);
    switch (dbret) {
        case 0:
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            break;
        default:
            /* some other db error, raise Exception */
            PyErr_Format (terane_Exc_Error, "Failed to find document %s: %s", 
                (char *) key.data, db_strerror (dbret));
            break;
    }
    if (dbret == 0)
        Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

/*
 * _Segment_next_doc: build a (docId,document) tuple from the current cursor item
 */
static PyObject *
_Segment_next_doc (terane_Iter *iter, DBT *key, DBT *data)
{
    PyObject *id, *document, *tuple;

    /* get the document id */
    id = PyString_FromString ((char *) key->data);
    /* get the document */
    document = PyString_FromString ((char *) data->data);
    /* build the (docnum,document) tuple */
    tuple = PyTuple_Pack (2, id, document);
    Py_XDECREF (id);
    Py_XDECREF (document);
    return tuple;
}

/*
 * terane_Segment_iter_docs: Iterate through all documents.
 *
 * callspec: Segment.iter_docs(txn)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 * returns: a new Iterator object.  Each iteration returns a tuple consisting
 *  of (docId,document).
 * exceptions:
 *   Exception: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_iter_docs (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    DBC *cursor = NULL;
    PyObject *iter = NULL;
    terane_Iter_ops ops = { .next = _Segment_next_doc };
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O", &txn))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");
    
    /* create a new cursor */
    dbret = self->documents->cursor (self->documents, txn? txn->txn : NULL, &cursor, 0);
    /* if cursor allocation failed, return Error */
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to allocate document cursor: %s",
            db_strerror (dbret));
        return NULL;
    }
    iter = terane_Iter_new ((PyObject *) self, cursor, &ops);
    if (iter == NULL)
        cursor->close (cursor);
    return iter;
}
