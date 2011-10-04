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
 * callspec: Segment.new_doc(txn, id)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   id (long): The document id
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.DocExists: The specified document id already exists
 *   terane.outputs.store.backend.Error: A db error occurred when trying to create the record
 */
PyObject *
terane_Segment_new_doc (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    terane_DID_num did_num;
    terane_DID_string did_string;
    DBT key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!K", &terane_TxnType, &txn, &did_num))
        return NULL;
    DID_num_to_string (did_num, did_string);
    /* put the record.  the record number is set in the key */
    memset (&key, 0, sizeof (DBT));
    memset (&data, 0, sizeof (DBT));
    key.data = &did_string;
    key.size = TERANE_DID_STRING_LEN;
    dbret = self->documents->put (self->documents, txn->txn, &key, &data, DB_NOOVERWRITE);
    switch (dbret) {
        case 0:
            break;
        case DB_KEYEXIST:
            return PyErr_Format (terane_Exc_DocExists,
                "Failed to create document: document ID already exists");
        default:
            return PyErr_Format (terane_Exc_Error, "Failed to create document: %s",
                db_strerror (dbret));
    }
    /* increment the internal document count */
    self->ndocuments += 1;
    Py_RETURN_NONE;
}

/*
 * terane_Segment_get_doc: retrieve a document
 *
 * callspec: Segment.get_doc(txn, id)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   id (long): The document id
 * returns: A string representing the document contents 
 * exceptions:
 *   KeyError: The document with the specified id doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to retrieve the record
 */
PyObject *
terane_Segment_get_doc (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    terane_DID_num did_num;
    terane_DID_string did_string;
    DBT key, data;
    PyObject *document = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OK", &txn, &did_num))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    /* use the document id as the record number */
    DID_num_to_string (did_num, did_string);
    memset (&key, 0, sizeof (DBT));
    key.data = did_string;
    key.size = TERANE_DID_STRING_LEN;
    /* get the record */
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
            /* some other db error, raise Exception */
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
 * callspec: Segment.set_doc(id, document)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   id (long): The document id
 *   document (string): Data to store in the document
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_set_doc (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    terane_DID_num did_num;
    terane_DID_string did_string;
    const char *document = NULL;
    DBT key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!Ks", &terane_TxnType, &txn, &did_num, &document))
        return NULL;
    /* use the document id as the record number */
    memset (&key, 0, sizeof (DBT));
    DID_num_to_string (did_num, did_string);
    key.data = did_string;
    key.size = TERANE_DID_STRING_LEN;
    /* set the document from the data parameter */
    memset (&data, 0, sizeof (DBT));
    data.data = (char *) document;
    data.size = strlen (document) + 1;
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
 * callspec: Segment.delete_doc(txn, id)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   id (long): The document id
 * returns: None
 * exceptions:
 *   KeyError: The document with the specified id doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to delete the record
 */
PyObject *
terane_Segment_delete_doc (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    terane_DID_num did_num;
    terane_DID_string did_string;
    DBT key;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!K", &terane_TxnType, &txn, &did_num))
        return NULL;
    /* get the document id */
    DID_num_to_string (did_num, did_string);
    memset (&key, 0, sizeof (DBT));
    key.data = did_string;
    key.size = TERANE_DID_STRING_LEN;
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
 * callspec: Segment.contains_doc(id)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   id (long): The document id
 * returns: True if the document exists, otherwise False.
 * exceptions:
 *   Exception: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_contains_doc (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    terane_DID_num did_num;
    terane_DID_string did_string;
    DBT key;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OK", &txn, &did_num))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    /* get the document id */
    DID_num_to_string (did_num, did_string);
    memset (&key, 0, sizeof (DBT));
    key.data = did_string;
    key.size = TERANE_DID_STRING_LEN;
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
 * _Segment_next_doc: build a (doc_id,doc) tuple from the current cursor item
 */
static PyObject *
_Segment_next_doc (terane_Iter *iter, DBT *key, DBT *data)
{
    terane_DID_num did_num;
    PyObject *id, *document, *tuple;

    DID_string_to_num ((char *) key->data, &did_num);
    /* get the document id */
    id = PyLong_FromUnsignedLongLong (did_num);
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
 *  of (docnum,document).
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
    iter = Iter_new ((PyObject *) self, cursor, &ops);
    if (iter == NULL)
        cursor->close (cursor);
    return iter;
}

/*
 * terane_Segment_count_docs: Return the number of documents in the db.
 *
 * callspec: Segment.count_docs()
 * parameters: None
 * returns: The number of documents in the segment.
 * exceptions: None
 */
PyObject *
terane_Segment_count_docs (terane_Segment *self)
{
    return PyLong_FromUnsignedLong (self->ndocuments);
}

/*
 * terane_Segment_first_doc: Return the first (lowest numbered) document.
 *
 * callspec: Segment.first_doc(txn)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 * returns: A tuple consisting of (docnum,document).
 * exceptions:
 *   IndexError: There are no documents in the Segment
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_first_doc (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    DBC *cursor = NULL;
    DBT key, data;
    PyObject *tuple = NULL;
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

    /* retrieve the first item */
    memset (&key, 0, sizeof (DBT));
    key.flags = DB_DBT_MALLOC;
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    dbret = cursor->get (cursor, &key, &data, DB_FIRST);
    switch (dbret) {
        case 0:
            tuple = _Segment_next_doc ((terane_Iter *)NULL, &key, &data);
            break;
        case DB_NOTFOUND:
            PyErr_Format (PyExc_IndexError, "Segment is empty");
            break;
        /* for any other error, set exception and return NULL */
        case DB_LOCK_DEADLOCK:
            PyErr_Format (terane_Exc_Deadlock, "Failed to get item: %s",
                db_strerror (dbret));
            break;
        case DB_LOCK_NOTGRANTED:
            PyErr_Format (terane_Exc_LockTimeout, "Failed to get item: %s",
                db_strerror (dbret));
            break;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to get item: %s",
                db_strerror (dbret));
            break;
    }

    /* free allocated memory */
    if (key.data)
        PyMem_Free (key.data);
    if (data.data)
        PyMem_Free (data.data);
    if (cursor != NULL)
        cursor->close (cursor);
    return tuple;
}

/*
 * terane_Segment_last_doc: Return the last (highest numbered) document.
 *
 * callspec: Segment.last_doc(txn)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 * returns: A tuple consisting of (docnum,document).
 * exceptions:
 *   IndexError: There are no documents in the Segment
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_last_doc (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    DBC *cursor = NULL;
    DBT key, data;
    PyObject *tuple = NULL;
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

    /* retrieve the first item */
    memset (&key, 0, sizeof (DBT));
    key.flags = DB_DBT_MALLOC;
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    dbret = cursor->get (cursor, &key, &data, DB_LAST);
    switch (dbret) {
        case 0:
            tuple = _Segment_next_doc ((terane_Iter *)NULL, &key, &data);
            break;
        case DB_NOTFOUND:
            PyErr_Format (PyExc_IndexError, "Segment is empty");
            break;
        /* for any other error, set exception and return NULL */
        case DB_LOCK_DEADLOCK:
            PyErr_Format (terane_Exc_Deadlock, "Failed to get item: %s",
                db_strerror (dbret));
            break;
        case DB_LOCK_NOTGRANTED:
            PyErr_Format (terane_Exc_LockTimeout, "Failed to get item: %s",
                db_strerror (dbret));
            break;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to get item: %s",
                db_strerror (dbret));
            break;
    }

    /* free allocated memory */
    if (key.data)
        PyMem_Free (key.data);
    if (data.data)
        PyMem_Free (data.data);
    if (cursor != NULL)
        cursor->close (cursor);
    return tuple;
}
