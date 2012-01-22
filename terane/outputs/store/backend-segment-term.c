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
 * _Segment_make_term_key:
 */
static DBT *
_Segment_make_term_key (PyObject *term, PyObject *id)
{
    PyObject *encoded = NULL;
    char *term_str, *doc_id;
    Py_ssize_t term_len, id_len;
    DBT *key = NULL;

    if (!PyUnicode_Check (term))
        return (DBT *) PyErr_Format (PyExc_TypeError, "Argument 'term' is not unicode type");
    if (id && !PyString_Check (id))
        return (DBT *) PyErr_Format (PyExc_TypeError, "Argument 'id' is not str type");

    /* convert term from UTF-16 to UTF-8 */
    encoded = PyUnicode_AsUTF8String (term);
    if (encoded == NULL)
        return NULL;        /* raises a codec error */
    /* get term string data and length */
    if (PyString_AsStringAndSize (encoded, &term_str, &term_len) < 0) {
        Py_DECREF (encoded);
        return NULL;        /* raises TypeError */
    }
    /* get document ID string and length */
    if (PyString_AsStringAndSize (id, &doc_id, &id_len) < 0) {
        Py_DECREF (encoded);
        return NULL;        /* raises TypeError */
    }
    /* allocate a DBT to store the key */
    key = PyMem_Malloc (sizeof (DBT));
    if (key == NULL) {
        PyErr_NoMemory ();
        Py_DECREF (encoded);
        return NULL;    /* raises MemoryError */
    }
    memset (key, 0, sizeof (DBT));

    /* create key in the form of '<' + term + '>' + id + '\0' */
    key->data = PyMem_Malloc (term_len + id_len + 3);
    if (key->data == NULL) {
        PyErr_NoMemory ();
        PyMem_Free (key);
        Py_DECREF (encoded);
        return NULL;    /* raises MemoryError */
    }
    key->size = term_len + id_len + 3;
    ((char *)key->data)[0] = '<';
    memcpy (key->data + 1, term_str, term_len);
    ((char *)key->data)[term_len + 1] = '>';
    memcpy (key->data + term_len + 2, doc_id, id_len);
    ((char *)key->data)[term_len + id_len + 2] = '\0';

    Py_DECREF (encoded);
    return key;
}

/*
 * _Segment_make_meta_key:
 */
static DBT *
_Segment_make_meta_key (PyObject *term)
{
    PyObject *encoded = NULL;
    char *term_str;
    Py_ssize_t term_len;
    DBT *key = NULL;

    assert (term != NULL);
    if (!PyUnicode_Check (term)) {
        PyErr_SetString (PyExc_TypeError, "Argument 'term' is not Unicode type");
        return NULL;
    }

    /* convert term from UTF-16 to UTF-8 */
    encoded = PyUnicode_AsUTF8String (term);
    if (encoded == NULL)
        return NULL;        /* raises a codec error */
    if (PyString_AsStringAndSize (encoded, &term_str, &term_len) < 0) {
        Py_DECREF (encoded);
        return NULL;        /* raises TypeError */
    }

    /* allocate a DBT to store the key */
    key = PyMem_Malloc (sizeof (DBT));
    if (key == NULL) {
        PyErr_NoMemory ();
        Py_DECREF (encoded);
        return NULL;    /* raises MemoryError */
    }
    memset (key, 0, sizeof (DBT));

    /* create key in the form of '!' + term + '\0' */
    key->data = PyMem_Malloc (term_len + 2);
    if (key->data == NULL) {
        PyErr_NoMemory ();
        PyMem_Free (key);
        Py_DECREF (encoded);
        return NULL;    /* raises MemoryError */
    }
    key->size = term_len + 2;
    ((char *)key->data)[0] = '!';
    memcpy (key->data + 1, term_str, term_len);
    ((char *)key->data)[term_len + 1] = '\0';

    Py_DECREF (encoded);
    return key;
}

/*
 * _Segment_make_iter_key:
 */
static DBT *
_Segment_make_iter_key (PyObject *term)
{
    PyObject *encoded = NULL;
    char *term_str;
    Py_ssize_t term_len;
    DBT *key = NULL;

    assert (term != NULL);
    if (!PyUnicode_Check (term)) {
        PyErr_SetString (PyExc_TypeError, "Argument 'term' is not Unicode type");
        return NULL;
    }

    /* convert term from UTF-16 to UTF-8 */
    encoded = PyUnicode_AsUTF8String (term);
    if (encoded == NULL)
        return NULL;        /* raises a codec error */
    if (PyString_AsStringAndSize (encoded, &term_str, &term_len) < 0) {
        Py_DECREF (encoded);
        return NULL;        /* raises TypeError */
    }

    /* allocate a DBT to store the key */
    key = PyMem_Malloc (sizeof (DBT));
    if (key == NULL) {
        PyErr_NoMemory ();
        Py_DECREF (encoded);
        return NULL;    /* raises MemoryError */
    }
    memset (key, 0, sizeof (DBT));

    /* create key in the form of '<' + term + '>' + '\0' */
    key->data = PyMem_Malloc (term_len + 3);
    if (key->data == NULL) {
        PyErr_NoMemory ();
        PyMem_Free (key);
        Py_DECREF (encoded);
        return NULL;    /* raises MemoryError */
    }
    /* 
     * the actual key is '<' + term + '>' without the \0 terminator.
     * however its easier to debug with a terminated string, so we add
     * it although it is not reflected in the DBT size field.
     */
    key->size = term_len + 2;
    ((char *)key->data)[0] = '<';
    memcpy (key->data + 1, term_str, term_len);
    ((char *)key->data)[term_len + 1] = '>';
    ((char *)key->data)[term_len + 2] = '\0';

    Py_DECREF (encoded);
    return key;
}

/*
 * _Segment_free_key:
 */
static void
_Segment_free_key (DBT *key)
{
    assert (key != NULL);
    if (key->data != NULL)
        PyMem_Free (key->data);
    PyMem_Free (key);
}

/*
 * terane_Segment_get_term:
 *
 * callspec: Segment.get_term(txn, fieldname, term, docId)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 *   term (unicode): The term
 *   docId (str): The document ID string
 * returns: The JSON-encoded posting data
 * exceptions:
 *   KeyError: The specified field or record doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_get_term (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *term = NULL;
    PyObject *id = NULL;
    DBT *key, data;
    DB *field;
    PyObject *value = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!O!", &txn, &PyString_Type, &fieldname,
        &PyUnicode_Type, &term, PyString_Type, &id))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    field = terane_Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the term and id values */
    key = _Segment_make_term_key (term, id);
    if (key == NULL)
        return NULL;

    /* get the record */
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    dbret = field->get (field, txn? txn->txn : NULL, key, &data, 0);
    _Segment_free_key (key);
    switch (dbret) {
        case 0:
            /* create a python string from the data */
            value = PyString_FromString (data.data);
            break;
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            /* document doesn't exist, raise KeyError */
            return PyErr_Format (PyExc_KeyError, "Data doesn't exist");
        default:
            /* some other db error, raise Error */
            return PyErr_Format (terane_Exc_Error, "Failed to get data: %s",
                db_strerror (dbret));
    }

    /* free allocated memory */
    if (data.data)
        PyMem_Free (data.data);
    return value;
}

/*
 * terane_Segment_set_term:
 *
 * callspec: Segment.set_term(txn, fieldname, term, docId, value)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   fieldname (string): The field name
 *   term (unicode): The term
 *   id (long): The document id
 *   value (unicode): JSON-encoded posting data
 * returns: None
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_set_term (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *term = NULL;
    PyObject *id = NULL;
    char *value = NULL;
    DBT *key, data;
    DB *field;
    int value_len = 0, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!O!O!O!s#", &terane_TxnType, &txn,
        &PyString_Type, &fieldname, &PyUnicode_Type, &term, &PyString_Type, &id,
        &value, &value_len))
        return NULL;

    field = terane_Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the term and id values */
    key = _Segment_make_term_key (term, id);
    if (key == NULL)
        return NULL;

    /* set the record */
    memset (&data, 0, sizeof (DBT));
    data.data = value;
    data.size = value_len + 1;
    dbret = field->put (field, txn->txn, key, &data, 0);
    _Segment_free_key (key);
    switch (dbret) {
        case 0:
            break;
        default:
            /* some other db error, raise Error */
            return PyErr_Format (terane_Exc_Error, "Failed to set data: %s",
                db_strerror (dbret));
    }
    Py_RETURN_NONE;
}

/*
 * terane_Segment_contains_term: Determine whether the specified field contains
 *  the specified term.
 *
 * callspec: Segment.contains_term(txn, fieldname, term)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 *   term (unicode): The term
 * returns: True if the term exists in the field, otherwise False.
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to find the record
 */
PyObject *
terane_Segment_contains_term (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *term = NULL;
    DBT *key;
    DB *field;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!", &txn, &PyString_Type, &fieldname,
        &PyUnicode_Type, &term))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    field = terane_Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the term value */
    key = _Segment_make_meta_key (term);
    if (key == NULL)
        return NULL;

    /* check if key exists in the db */
    dbret = field->exists (field, txn? txn->txn : NULL, key, 0);
    _Segment_free_key (key);
    switch (dbret) {
        case 0:
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            break;
        default:
            /* some other db error, raise Exception */
            PyErr_Format (terane_Exc_Error, "Failed to find term: %s",
                db_strerror (dbret));
            break;
    }
    if (dbret == 0)
        Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

/*
 * terane_Segment_estimate_term_postings: Return an estimate of the number of postings
 *  for a term between the start and end document ID.
 *
 * callspec: Segment.estimate_term_count(txn, fieldname, term, start, end)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 *   term (unicode): The term
 *   start (str): The starting document ID
 *   end (str): The ending document ID
 * returns: An estimate of the percentage of total postings of all terms in the field which
 *   are between the start and end document IDs, expressed as a float.
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to find the record
 */
PyObject *
terane_Segment_estimate_term_postings (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *term = NULL, *start = NULL, *end = NULL;
    DBT *key;
    DB *field;
    DB_KEY_RANGE startrange, endrange;
    double estimate = 0.0;
    int dbret, result;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!O!O!", &txn, &PyString_Type, &fieldname,
        &PyUnicode_Type, &term, &PyString_Type, &start, &PyString_Type, &end))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    field = terane_Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the start key from the term value */
    key = _Segment_make_term_key (term, start);
    if (key == NULL)
        return NULL;

    /* estimate start key range */
    dbret = field->key_range (field, txn? txn->txn : NULL, key, &startrange, 0);
    _Segment_free_key (key);
    if (dbret != 0) 
        return PyErr_Format (terane_Exc_Error, "Failed to estimate start key range: %s",
            db_strerror (dbret));

    /* build the end key from the term value */
    key = _Segment_make_term_key (term, end);
    if (key == NULL)
        return NULL;

    /* estimate start key range */
    dbret = field->key_range (field, txn? txn->txn : NULL, key, &endrange, 0);
    _Segment_free_key (key);
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
 * _Segment_next_term: return the (id,value) tuple from the current cursor item
 */
static PyObject *
_Segment_next_term (terane_Iter *iter, DBT *key, DBT *data)
{
    PyObject *id, *value, *tuple;
    char *doc_id = NULL;
    int i;

    /* find the term end marker */
    for (i = key->size - 1; i >= 0; i--) {
        doc_id = key->data + i;
        if (*doc_id == '>')
            break;
    }
    if (*doc_id != '>')
        return NULL;
    doc_id++;
    /* get the document id */
    id = PyString_FromString(doc_id);
    /* get the metadata */
    value = PyString_FromString ((char *) data->data);
    /* build the (id,metadata) tuple */
    tuple = PyTuple_Pack (2, id, value);
    Py_XDECREF (id);
    Py_XDECREF (value);
    return tuple;
}

/*
 * _Segment_skip_term: create a key to skip to the item specified by id.
 */
static DBT *
_Segment_skip_term (terane_Iter *iter, PyObject *args)
{
    char *doc_id = NULL;
    int id_len = 0;
    DBT *key = NULL;

    if (!PyArg_ParseTuple (args, "s#", &doc_id, &id_len))
        return NULL;
    /* allocate a DBT to store the key */
    key = PyMem_Malloc (sizeof (DBT));
    if (key == NULL)
        return (void *) PyErr_NoMemory ();
    memset (key, 0, sizeof (DBT));
    /* id_len does not include the trailing '\0' */
    key->size = iter->start_key.size + id_len + 1;
    /* iter->key is in the form of '<' + term + '>', without the '\0' */
    key->data = PyMem_Malloc (key->size);
    if (key->data == NULL) {
        PyErr_NoMemory ();
        PyMem_Free (key);
        return NULL;    /* raises MemoryError */
    }
    /* create key in the form of '<' + term + '>' + id + '\0' */
    memcpy (key->data, iter->start_key.data, iter->start_key.size);
    memcpy (key->data + iter->start_key.size, doc_id, id_len);
    return key;
}

/*
 * terane_Segment_iter_terms: Iterate through all document ids associated
 *  with the specified term in the specified field.
 *
 * callspec: Segment.iter_terms(txn, fieldname, term)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 *   term (string): The term
 * returns: a new Iterator object.  Each iteration returns a tuple consisting
 *  of (docId,value).
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_iter_terms (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *term = NULL;
    DB *field = NULL;
    DBT *key = NULL;
    DBC *cursor = NULL;
    int dbret;
    PyObject *iter = NULL;
    terane_Iter_ops ops = { .next = _Segment_next_term, .skip = _Segment_skip_term };

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!", &txn, &PyString_Type, &fieldname,
        &PyUnicode_Type, &term))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    field = terane_Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the term value */
    key = _Segment_make_iter_key (term);
    if (key == NULL)
        return NULL;

    /* create a new cursor */
    dbret = field->cursor (field, txn? txn->txn : NULL, &cursor, 0);
    /* if cursor allocation failed, return Error */
    if (dbret != 0) {
        _Segment_free_key (key);
        return PyErr_Format (terane_Exc_Error, "Failed to allocate DB cursor: %s",
            db_strerror (dbret));
    }

    iter = terane_Iter_new_range ((PyObject *) self, cursor, &ops, key->data, key->size);
    _Segment_free_key (key);
    if (iter == NULL)
        cursor->close (cursor);
    return iter;
}

/*
 * _Segment_skip_term_within: create a key to skip to the item specified by id.
 */
static DBT *
_Segment_skip_term_within (terane_Iter *iter, PyObject *args)
{
    char *doc_id = NULL;
    int id_len = 0, i;
    DBT *key = NULL;

    if (!PyArg_ParseTuple (args, "s#", &doc_id, &id_len))
        return NULL;
    /* find the term end marker */
    for (i = iter->start_key.size - 1; i >= 0; i--) {
        if (((char *)iter->start_key.data)[i] == '>')
            break;
    }
    if (i < 0)
        return NULL;
    /* allocate a DBT to store the key */
    key = PyMem_Malloc (sizeof (DBT));
    if (key == NULL)
        return (void *) PyErr_NoMemory ();
    memset (key, 0, sizeof (DBT));
    /* id_len does not include the trailing '\0' */
    key->size = i + 1 + id_len + 1;
    /* iter->key is in the form of '<' + term + '>', without the '\0' */
    key->data = PyMem_Malloc (key->size);
    if (key->data == NULL) {
        PyErr_NoMemory ();
        PyMem_Free (key);
        return NULL;    /* raises MemoryError */
    }
    /* create key in the form of '<' + term + '>' + id + '\0' */
    memset (key->data, 0, key->size);
    memcpy (key->data, iter->start_key.data, i + 1);
    memcpy (key->data + i + 1, doc_id, id_len);
    return key;
}

/*
 * terane_Segment_iter_terms_within: Iterate through all document ids associated
 *  with the specified term in the specified field between the specified start and
 *  end document IDs.
 *
 * callspec: Segment.iter_terms_within(txn, fieldname, term, start, end)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 *   term (str): The term
 *   start (str): The starting document ID, inclusive
 *   end (str): The ending document ID, inclusive
 * returns: a new Iterator object.  Each iteration returns a tuple consisting
 *  of (docId,value).
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_iter_terms_within (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *term = NULL, *start = NULL, *end = NULL;
    DB *field = NULL;
    DBT *start_key = NULL, *end_key = NULL;
    DBC *cursor = NULL;
    int dbret;
    PyObject *iter = NULL;
    terane_Iter_ops ops = { .next = _Segment_next_term, .skip = _Segment_skip_term_within };

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!O!O!", &txn, &PyString_Type, &fieldname,
        &PyUnicode_Type, &term, &PyString_Type, &start, &PyString_Type, &end))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    field = terane_Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the start and end keys */
    start_key = _Segment_make_term_key (term, start);
    if (start_key == NULL)
        goto error;
    end_key = _Segment_make_term_key (term, end);
    if (end_key == NULL)
        goto error;

    /* create a new cursor */
    dbret = field->cursor (field, txn? txn->txn : NULL, &cursor, 0);
    /* if cursor allocation failed, return Error */
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to allocate DB cursor: %s",
            db_strerror (dbret));
        goto error;
    }
    iter = terane_Iter_new_within ((PyObject *) self, cursor, &ops, start_key, end_key);
    _Segment_free_key (start_key);
    _Segment_free_key (end_key);
    if (iter == NULL)
        cursor->close (cursor);
    return iter;

error:
    if (start_key)
        _Segment_free_key (start_key);
    if (end_key)
        _Segment_free_key (end_key);
    if (cursor)
        cursor->close (cursor);
    return NULL;
}


/*
 * terane_Segment_get_term_meta: Retrieve the metadata associated with the
 *  specified term in the specified field.
 *
 * callspec: Segment.get_term_meta(txn, fieldname, term)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 *   term (unicode): The term
 * returns: a string containing the JSON-encoded metadata. 
 * exceptions:
 *   KeyError: The specified field or metadata doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_get_term_meta (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *term = NULL;
    DBT *key, data;
    DB *field;
    PyObject *metadata = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!", &txn, &PyString_Type, &fieldname,
        &PyUnicode_Type, &term))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    field = terane_Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the term value */
    key = _Segment_make_meta_key (term);
    if (key == NULL)
        return NULL;

    /* get the record */
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    dbret = field->get (field, txn? txn->txn : NULL, key, &data, 0);
    _Segment_free_key (key);
    switch (dbret) {
        case 0:
            /* create a python string from the data */
            metadata = PyString_FromString (data.data);
            break;
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            /* document doesn't exist, raise KeyError */
            return PyErr_Format (PyExc_KeyError, "Metadata doesn't exist");
        default:
            /* some other db error, raise Error */
            return PyErr_Format (terane_Exc_Error, "Failed to get metadata: %s",
                db_strerror (dbret));
    }
    
    /* free allocated memory */
    if (data.data)
        PyMem_Free (data.data);
    return metadata;
}

/*
 * terane_Segment_set_term_meta: Change the metadata associated with the
 *  specified term in the specified field.
 *
 * callspec: Segment.set_term_meta(txn, fieldname, term, metadata)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   fieldname (string): The field name
 *   term (unicode): The term
 *   metadata (string): The JSON-encoded metadata
 * returns: None
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_set_term_meta (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *term = NULL;
    const char *metadata = NULL;
    DBT *key, data;
    DB *field;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!O!O!s", &terane_TxnType, &txn,
        &PyString_Type, &fieldname, &PyUnicode_Type, &term, &metadata))
        return NULL;

    field = terane_Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the term value */
    key = _Segment_make_meta_key (term);
    if (key == NULL)
        return NULL;

    /* set the record */
    memset (&data, 0, sizeof (DBT));
    data.data = (char *) metadata;
    data.size = strlen (metadata) + 1;
    dbret = field->put (field, txn->txn, key, &data, 0);
    _Segment_free_key (key);
    switch (dbret) {
        case 0:
            break;
        default:
            /* some other db error, raise Error */
            return PyErr_Format (terane_Exc_Error, "Failed to set metadata: %s",
                db_strerror (dbret));
    }
    Py_RETURN_NONE;
}

/*
 * _Segment_next_term_meta: return the (term,metadata) tuple from the current cursor item
 */
static PyObject *
_Segment_next_term_meta (terane_Iter *iter, DBT *key, DBT *data)
{
    PyObject *term, *metadata, *tuple;

    /* if key is empty then stop iterating */
    if (key->size == 0)
        return NULL;
    /* if key doesn't start with a '!', then stop iterating */
    if (((char *) key->data)[0] != '!')
        return NULL;
    /* decode UTF-8 term data, excluding the leading '!' and the trailing '\0' */
    term = PyUnicode_DecodeUTF8 ((char *) &((char *)key->data)[1],
        key->size - 2, "strict");
    /* if term is NULL, then there was a problem decoding the term from UTF-8 */
    if (term == NULL)
        return NULL;
    /* get the metadata */
    metadata = PyString_FromString ((char *) data->data);
    /* build the (term,metadata) tuple */
    tuple = PyTuple_Pack (2, term, metadata);
    Py_XDECREF (term);
    Py_XDECREF (metadata);
    return tuple;
}

/*
 * terane_Segment_iter_terms_meta: Iterate through all terms in the specified
 *  field and return the metadata for each term.
 *
 * callspec: Segments.iter_terms_meta(txn, fieldname)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 * returns: a new Iterator object.  Each iteration returns a tuple consisting
 *  of (term,metadata).
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_iter_terms_meta (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    DBC *cursor = NULL;
    DB *field;
    int dbret;
    PyObject *iter = NULL;
    terane_Iter_ops ops = { .next = _Segment_next_term_meta };

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!", &txn, &PyString_Type, &fieldname))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    field = terane_Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* create a new cursor */
    dbret = field->cursor (field, txn? txn->txn : NULL, &cursor, 0);
    /* if cursor allocation failed, return Error */
    if (dbret != 0)
        return PyErr_Format (terane_Exc_Error, "Failed to allocate DB cursor: %s",
            db_strerror (dbret));
    iter = terane_Iter_new_range ((PyObject *) self, cursor, &ops, (void *)"!", 1);
    if (iter == NULL)
        cursor->close (cursor);
    return iter;
}

/*
 * terane_Segment_iter_term_meta_range: Iterate through all terms matching the
 *  specified prefix in the specified field and return the metadata for each term.
 *
 * callspec: Segment.iter_term_meta_range(fieldname, prefix)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 *   prefix (unicode): The term prefix to match
 * returns: a new Iterator object.  Each iteration returns a tuple consisting
 *  of (term,metadata).
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_iter_terms_meta_range (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *prefix = NULL;
    DB *field = NULL;
    DBT *key = NULL;
    DBC *cursor = NULL;
    int dbret;
    PyObject *iter = NULL;
    terane_Iter_ops ops = { .next = _Segment_next_term_meta };

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!", &txn, &PyString_Type, &fieldname,
        &PyUnicode_Type, &prefix))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    field = terane_Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the term value */
    key = _Segment_make_meta_key (prefix);
    if (key == NULL)
        return NULL;

    /* create a new cursor */
    dbret = field->cursor (field, txn? txn->txn : NULL, &cursor, 0);
    /* if cursor allocation failed, return Error */
    if (dbret != 0) {
        _Segment_free_key (key);
        return PyErr_Format (terane_Exc_Error, "Failed to allocate DB cursor: %s",
            db_strerror (dbret));
    }
    iter = terane_Iter_new_range ((PyObject *) self, cursor, &ops, key->data, key->size - 1);
    if (iter == NULL)
        cursor->close (cursor);
    _Segment_free_key (key);
    return iter;
}
