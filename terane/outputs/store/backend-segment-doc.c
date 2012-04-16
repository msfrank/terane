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
 * _Segment_make_doc_key:
 */
static DBT *
_Segment_make_doc_key (PyObject *id)
{
    char *id_str;
    Py_ssize_t id_len;
    DBT *key = NULL;

    assert (id != NULL);

    if (!PyString_Check (id))
        return (DBT *) PyErr_Format (PyExc_TypeError, "Argument 'id' is not str type");

    /* get document ID string and length */
    if (PyString_AsStringAndSize (id, &id_str, &id_len) < 0)
        return NULL;        /* raises TypeError */

    /* allocate a DBT to store the key */
    key = PyMem_Malloc (sizeof (DBT));
    if (key == NULL)
        return (DBT *) PyErr_NoMemory ();
    memset (key, 0, sizeof (DBT));

    /* initialize the DBT */
    key->size = id_len + 1;
    key->ulen = key->size;
    key->flags = DB_DBT_USERMEM;
    key->data = PyMem_Malloc (key->size);
    if (key->data == NULL) {
        PyMem_Free (key);
        return (DBT *) PyErr_NoMemory ();
    }
    strncpy ((char *) key->data, id_str, id_len + 1);
    return key;
}

/*
 * _Segment_free_doc_key:
 */
static void
_Segment_free_doc_key (DBT *key)
{
    assert (key != NULL);
    if (key->data != NULL)
        PyMem_Free (key->data);
    PyMem_Free (key);
}

/*
 * terane_Segment_new_doc: create a new document
 *
 * callspec: Segment.new_doc(txn, evid)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   evid (str): The event identifier string
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
    /* put a new document.  raise DocExists if the event identifier already exists. */
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
 * callspec: Segment.get_doc(txn, evid)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   evid (str): The event identifier string
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
 * callspec: Segment.set_doc(txn, evid, document)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   evid (str): The event identifier string
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
    char *document = NULL;
    DBT key, data;
    int id_len = 0, doc_len = 0, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!s#s#", &terane_TxnType, &txn, &doc_id, &id_len, &document, &doc_len))
        return NULL;
    /* use the document id as the record number */
    memset (&key, 0, sizeof (DBT));
    key.data = doc_id;
    key.size = id_len + 1;
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
 * callspec: Segment.delete_doc(txn, evid)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   evid (str): The event identifier string
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
 * callspec: Segment.contains_doc(txn, evid)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   evid (str): The event identifier string
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
 * terane_Segment_estimate_docs: Return an estimate of the number of documents
 *  between the start and end document IDs.
 *
 * callspec: Segment.estimate_docs(txn, start, end)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   start (str): The starting document ID
 *   end (str): The ending document ID
 * returns: An estimate of the percentage of documents between the start and end IDs, expressed as a float.
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to find the record
 */
PyObject *
terane_Segment_estimate_docs (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *start = NULL, *end = NULL;
    DBT *key;
    DB_KEY_RANGE startrange, endrange;
    double estimate = 0.0;
    int dbret, result;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!", &txn, &PyString_Type, &start, &PyString_Type, &end))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* estimate start key range */
    key = _Segment_make_doc_key (start);
    if (key == NULL)
        return NULL;
    dbret = self->documents->key_range (self->documents, txn? txn->txn : NULL, key, &startrange, 0);
    _Segment_free_doc_key (key);
    if (dbret != 0) 
        return PyErr_Format (terane_Exc_Error, "Failed to estimate start key range: %s",
            db_strerror (dbret));

    /* estimate end key range */
    key = _Segment_make_doc_key (end);
    if (key == NULL)
        return NULL;
    dbret = self->documents->key_range (self->documents, txn? txn->txn : NULL, key, &endrange, 0);
    _Segment_free_doc_key (key);
    if (dbret != 0) 
        return PyErr_Format (terane_Exc_Error, "Failed to estimate end key range: %s",
            db_strerror (dbret));

    if (PyObject_Cmp (start, end, &result) < 0)
        return PyErr_Format (terane_Exc_Error, "key comparison failed");
    if (result > 0)
        estimate = 1.0 - (endrange.less + startrange.greater);
    else
        estimate = 1.0 - (startrange.less + endrange.greater);
    return PyFloat_FromDouble (estimate);
}
/*
 * _Segment_next_doc: build a (evid,document) tuple from the current cursor item
 */
static PyObject *
_Segment_next_doc (terane_Iter *iter, DBT *key, DBT *data)
{
    PyObject *id, *tuple;

    /* get the event id */
    id = PyString_FromString ((char *) key->data);
    /* build the (evid,None) tuple */
    tuple = PyTuple_Pack (2, id, Py_None);
    Py_XDECREF (id);
    return tuple;
}

/*
 * terane_Segment_iter_docs_within: Iterate through all documents between the
 * specified start and end IDs.
 *
 * callspec: Segment.iter_docs(txn)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   start (str): The starting document ID, inclusive
 *   end (str): The ending document ID, inclusive
 * returns: a new Iterator object.  Each iteration returns a tuple consisting
 *  of (evid,document).
 * exceptions:
 *   Exception: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_iter_docs_within (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *start = NULL, *end = NULL;
    DBC *cursor = NULL;
    DBT *start_key = NULL, *end_key = NULL;
    PyObject *iter = NULL;
    int dbret, reverse = 0;
    terane_Iter_ops ops = { .next = _Segment_next_doc };

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!", &txn, &PyString_Type, &start, &PyString_Type, &end))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* determine whether ordering is reversed */
    if (PyObject_Cmp (start, end, &reverse) < 0)
        return PyErr_Format (terane_Exc_Error, "comparison of start and end failed");
    if (reverse < 0)
        reverse = 0;

    /* build the start and end keys */
    start_key = _Segment_make_doc_key (start);
    if (start_key == NULL)
        goto error;
    end_key = _Segment_make_doc_key (end);
    if (end_key == NULL)
        goto error;

    /* create a new cursor */
    dbret = self->documents->cursor (self->documents, txn? txn->txn : NULL, &cursor, 0);
    /* if cursor allocation failed, return Error */
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to allocate document cursor: %s",
            db_strerror (dbret));
        goto error;
    }

    /* allocate a new Iter object */
    iter = terane_Iter_new_within ((PyObject *) self, cursor, &ops, start_key, end_key, reverse);
    if (iter == NULL)
        goto error;

    /* clean up and return the Iter */
    _Segment_free_doc_key (start_key);
    _Segment_free_doc_key (end_key);
    return iter;

error:
    if (start_key)
        _Segment_free_doc_key (start_key);
    if (end_key)
        _Segment_free_doc_key (end_key);
    if (cursor)
        cursor->close (cursor);
    return NULL;
}
