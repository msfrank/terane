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

#include "storage.h"

static int
_Segment_get_string (PyObject *obj, char **str, Py_ssize_t *len)
{
    int i, ret;

    ret = PyString_AsStringAndSize (obj, str, len);
    if (ret < 0)
        return -1;  /* raises TypeError */
    for (i = 0; i < *len; i++) {
        if ((*str)[i] == '\0')
            fprintf (stderr, "embedded null at byte %i\n", i);
    }
    return ret;
}

/*
 * _Segment_make_word_key:
 */
static DBT *
_Segment_make_word_key (PyObject *word, PyObject *id)
{
    PyObject *encoded = NULL;
    char *word_str;
    Py_ssize_t word_len;
    terane_DID_num did_num;
    terane_DID_string did_string;
    DBT *key = NULL;

    assert (word != NULL);
    if (!PyUnicode_Check (word)) {
        PyErr_SetString (PyExc_TypeError, "Argument 'word' is not unicode type");
        return NULL;
    }
    if (id && !PyLong_Check (id)) {
        PyErr_SetString (PyExc_TypeError, "Argument 'id' is not long type");
        return NULL;
    }

    /* convert word from UTF-16 to UTF-8 */
    encoded = PyUnicode_AsUTF8String (word);
    if (encoded == NULL)
        return NULL;        /* raises a codec error */
    /* get word string data and length */
    if (_Segment_get_string (encoded, &word_str, &word_len) < 0) {
        Py_DECREF (encoded);
        return NULL;        /* raises TypeError */
    }
    /* convert document id to a string */
    did_num = PyLong_AsUnsignedLongLong (id);
    DID_num_to_string (did_num, did_string);

    /* allocate a DBT to store the key */
    key = PyMem_Malloc (sizeof (DBT));
    if (key == NULL) {
        PyErr_NoMemory ();
        Py_DECREF (encoded);
        return NULL;    /* raises MemoryError */
    }
    memset (key, 0, sizeof (DBT));

    /* create key in the form of '<' + word + '>' + id + '\0' */
    key->data = PyMem_Malloc (word_len + TERANE_DID_STRING_LEN + 2);
    if (key->data == NULL) {
        PyErr_NoMemory ();
        PyMem_Free (key);
        Py_DECREF (encoded);
        return NULL;    /* raises MemoryError */
    }
    key->size = word_len + TERANE_DID_STRING_LEN + 2;
    ((char *)key->data)[0] = '<';
    memcpy (key->data + 1, word_str, word_len);
    ((char *)key->data)[word_len + 1] = '>';
    memcpy (key->data + word_len + 2, did_string, TERANE_DID_STRING_LEN);
    ((char *)key->data)[word_len + TERANE_DID_STRING_LEN + 1] = '\0';

    Py_DECREF (encoded);
    return key;
}

/*
 * _Segment_make_meta_key:
 */
static DBT *
_Segment_make_meta_key (PyObject *word)
{
    PyObject *encoded = NULL;
    char *word_str;
    Py_ssize_t word_len;
    DBT *key = NULL;

    assert (word != NULL);
    if (!PyUnicode_Check (word)) {
        PyErr_SetString (PyExc_TypeError, "Argument 'word' is not Unicode type");
        return NULL;
    }

    /* convert word from UTF-16 to UTF-8 */
    encoded = PyUnicode_AsUTF8String (word);
    if (encoded == NULL)
        return NULL;        /* raises a codec error */
    if (_Segment_get_string (encoded, &word_str, &word_len) < 0) {
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

    /* create key in the form of '!' + word + '\0' */
    key->data = PyMem_Malloc (word_len + 2);
    if (key->data == NULL) {
        PyErr_NoMemory ();
        PyMem_Free (key);
        Py_DECREF (encoded);
        return NULL;    /* raises MemoryError */
    }
    key->size = word_len + 2;
    ((char *)key->data)[0] = '!';
    memcpy (key->data + 1, word_str, word_len);
    ((char *)key->data)[word_len + 1] = '\0';

    Py_DECREF (encoded);
    return key;
}

/*
 * _Segment_make_iter_key:
 */
static DBT *
_Segment_make_iter_key (PyObject *word)
{
    PyObject *encoded = NULL;
    char *word_str;
    Py_ssize_t word_len;
    DBT *key = NULL;

    assert (word != NULL);
    if (!PyUnicode_Check (word)) {
        PyErr_SetString (PyExc_TypeError, "Argument 'word' is not Unicode type");
        return NULL;
    }

    /* convert word from UTF-16 to UTF-8 */
    encoded = PyUnicode_AsUTF8String (word);
    if (encoded == NULL)
        return NULL;        /* raises a codec error */
    if (_Segment_get_string (encoded, &word_str, &word_len) < 0) {
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

    /* create key in the form of '<' + word + '>' + '\0' */
    key->data = PyMem_Malloc (word_len + 3);
    if (key->data == NULL) {
        PyErr_NoMemory ();
        PyMem_Free (key);
        Py_DECREF (encoded);
        return NULL;    /* raises MemoryError */
    }
    /* 
     * the actual key is '<' + word + '>' without the \0 terminator.
     * however its easier to debug with a terminated string, so we add
     * it although it is not reflected in the DBT size field.
     */
    key->size = word_len + 2;
    ((char *)key->data)[0] = '<';
    memcpy (key->data + 1, word_str, word_len);
    ((char *)key->data)[word_len + 1] = '>';
    ((char *)key->data)[word_len + 2] = '\0';

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
 * terane_Segment_get_word:
 *
 * callspec: Segment.get_word(txn, fieldname, word, id)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 *   word (unicode): The word
 *   id (long): The document id
 * returns: The JSON-encoded data
 * exceptions:
 *   KeyError: The specified field or record doesn't exist
 *   terane.db.storage.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_get_word (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *word = NULL;
    PyObject *id = NULL;
    DBT *key, data;
    DB *field;
    PyObject *metadata = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!O!", &txn, &PyString_Type, &fieldname,
        &PyUnicode_Type, &word, PyLong_Type, &id))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* if the field doesn't exist set KeyError and return */
    field = Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the word and id values */
    key = _Segment_make_word_key (word, id);
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
            return PyErr_Format (PyExc_KeyError, "Data doesn't exist");
        default:
            /* some other db error, raise Error */
            return PyErr_Format (terane_Exc_Error, "Failed to get data: %s",
                db_strerror (dbret));
    }

    /* free allocated memory */
    if (data.data)
        PyMem_Free (data.data);
    return metadata;
}

/*
 * terane_Segment_set_word:
 *
 * callspec: Segment.set_word(txn, fieldname, word, id, data)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   fieldname (string): The field name
 *   word (unicode): The word
 *   id (long): The document id
 *   metadata (string): JSON-encoded data
 * returns: None
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.db.storage.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_set_word (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *word = NULL;
    PyObject *id = NULL;
    const char *metadata = NULL;
    DBT *key, data;
    DB *field;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!O!O!O!s", &terane_TxnType, &txn,
        &PyString_Type, &fieldname, &PyUnicode_Type, &word, &PyLong_Type, &id,
        &metadata))
        return NULL;

    /* if the field doesn't exist set KeyError and return */
    field = Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the word and id values */
    key = _Segment_make_word_key (word, id);
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
            return PyErr_Format (terane_Exc_Error, "Failed to set data: %s",
                db_strerror (dbret));
    }
    Py_RETURN_NONE;
}

/*
 * terane_Segment_contains_word: Determine whether the specified field contains
 *  the specified word.
 *
 * callspec: Segment.contains_word(txn, fieldname, word)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 *   word (unicode): The word
 * returns: True if the word exists in the field, otherwise False.
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.db.storage.Error: A db error occurred when trying to find the record
 */
PyObject *
terane_Segment_contains_word (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *word = NULL;
    DBT *key;
    DB *field;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!", &txn, &PyString_Type, &fieldname,
        &PyUnicode_Type, &word))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* if the field doesn't exist set KeyError and return */
    field = Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the word value */
    key = _Segment_make_meta_key (word);
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
            PyErr_Format (terane_Exc_Error, "Failed to find word: %s",
                db_strerror (dbret));
            break;
    }
    if (dbret == 0)
        Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

/*
 * _Segment_next_word: return the (id,metadata) tuple from the current cursor item
 */
static PyObject *
_Segment_next_word (terane_Iter *iter, DBT *key, DBT *data)
{
    terane_DID_num did_num;
    PyObject *id, *metadata, *tuple;
    char c;

    /* verify the key is long enough */
    if (key->size < 20)
        return NULL;
    /* verify the word end marker is present */
    c = ((char *)key->data)[key->size - 18];
    if (c != '>')
        return NULL;
    /* get the document id */
    DID_string_to_num (&((char *)key->data)[key->size - 17], &did_num);
    id = PyLong_FromUnsignedLongLong (did_num);
    /* get the metadata */
    metadata = PyString_FromString ((char *) data->data);
    /* build the (id,metadata) tuple */
    tuple = PyTuple_Pack (2, id, metadata);
    Py_XDECREF (id);
    Py_XDECREF (metadata);
    return tuple;
}

/*
 * _Segment_skip_word: create a key to skip to the item specified by id.
 */
static DBT *
_Segment_skip_word (terane_Iter *iter, PyObject *args)
{
    terane_DID_num did_num;
    terane_DID_string did_string;
    DBT *key = NULL;

    if (!PyArg_ParseTuple (args, "K", (unsigned PY_LONG_LONG *) &did_num))
        return NULL;
    /* convert document id to a string */
    DID_num_to_string (did_num, did_string);

    /* allocate a DBT to store the key */
    key = PyMem_Malloc (sizeof (DBT));
    if (key == NULL)
        return (void *) PyErr_NoMemory ();
    memset (key, 0, sizeof (DBT));
    /* TERANE_DID_STRING_LEN includes the trailing '\0' */
    key->size = iter->len + TERANE_DID_STRING_LEN;
    /* iter->key is in the form of '<' + word + '>', without the '\0' */
    key->data = PyMem_Malloc (key->size);
    if (key->data == NULL) {
        PyErr_NoMemory ();
        PyMem_Free (key);
        return NULL;    /* raises MemoryError */
    }
    /* create key in the form of '<' + word + '>' + id + '\0' */
    memcpy (key->data, iter->key, iter->len);
    memcpy (key->data + iter->len, did_string, TERANE_DID_STRING_LEN);
    return key;
}

/*
 * terane_Segment_iter_words: Iterate through all document ids associated
 *  with the specified word in the specified field.
 *
 * callspec: Segment.iter_words(txn, fieldname, word)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 *   word (string): The word
 * returns: a new Iterator object.  Each iteration returns a tuple consisting
 *  of (id,metadata).
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.db.storage.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_iter_words (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *word = NULL;
    DB *field = NULL;
    DBT *key = NULL;
    DBC *cursor = NULL;
    int dbret;
    PyObject *iter = NULL;
    terane_Iter_ops ops = { .next = _Segment_next_word, .skip = _Segment_skip_word };

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!", &txn, &PyString_Type, &fieldname,
        &PyUnicode_Type, &word))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* if the field doesn't exist set KeyError and return */
    field = Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the word value */
    key = _Segment_make_iter_key (word);
    if (key == NULL)
        return NULL;

    /* create a new cursor */
    dbret = field->cursor (field, txn? txn->txn : NULL, &cursor, 0);
    /* if cursor allocation failed, return Error */
    if (dbret != 0)
        return PyErr_Format (terane_Exc_Error, "Failed to allocate DB cursor: %s",
            db_strerror (dbret));
    iter = Iter_new_range ((PyObject *) self, cursor, &ops, key->data, key->size);
    _Segment_free_key (key);
    if (iter == NULL)
        cursor->close (cursor);
    return iter;
}

/*
 * terane_Segment_get_word_meta: Retrieve the metadata associated with the
 *  specified word in the specified field.
 *
 * callspec: Segment.get_word_meta(txn, fieldname, word)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 *   word (unicode): The word
 * returns: a string containing the JSON-encoded metadata. 
 * exceptions:
 *   KeyError: The specified field or metadata doesn't exist
 *   terane.db.storage.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_get_word_meta (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *word = NULL;
    DBT *key, data;
    DB *field;
    PyObject *metadata = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!", &txn, &PyString_Type, &fieldname,
        &PyUnicode_Type, &word))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* if the field doesn't exist set KeyError and return */
    field = Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the word value */
    key = _Segment_make_meta_key (word);
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
 * terane_Segment_set_word_meta: Change the metadata associated with the
 *  specified word in the specified field.
 *
 * callspec: Segment.set_word_meta(txn, fieldname, word, metadata)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   fieldname (string): The field name
 *   word (unicode): The word
 *   metadata (string): The JSON-encoded metadata
 * returns: None
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.db.storage.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_set_word_meta (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *word = NULL;
    const char *metadata = NULL;
    DBT *key, data;
    DB *field;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!O!O!s", &terane_TxnType, &txn,
        &PyString_Type, &fieldname, &PyUnicode_Type, &word, &metadata))
        return NULL;

    /* if the field doesn't exist set KeyError and return */
    field = Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the word value */
    key = _Segment_make_meta_key (word);
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
 * _Segment_next_word_meta: return the (word,metadata) tuple from the current cursor item
 */
static PyObject *
_Segment_next_word_meta (terane_Iter *iter, DBT *key, DBT *data)
{
    PyObject *word, *metadata, *tuple;

    /* if key is empty then stop iterating */
    if (key->size == 0)
        return NULL;
    /* if key doesn't start with a '!', then stop iterating */
    if (((char *) key->data)[0] != '!')
        return NULL;
    /* decode UTF-8 word data, excluding the leading '!' and the trailing '\0' */
    word = PyUnicode_DecodeUTF8 ((char *) &((char *)key->data)[1],
        key->size - 2, "strict");
    /* if word is NULL, then there was a problem decoding the word from UTF-8 */
    if (word == NULL)
        return NULL;
    /* get the metadata */
    metadata = PyString_FromString ((char *) data->data);
    /* build the (word,metadata) tuple */
    tuple = PyTuple_Pack (2, word, metadata);
    Py_XDECREF (word);
    Py_XDECREF (metadata);
    return tuple;
}

/*
 * terane_Segment_iter_words_meta: Iterate through all words in the specified
 *  field and return the metadata for each word.
 *
 * callspec: Segments.iter_words_meta(txn, fieldname)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 * returns: a new Iterator object.  Each iteration returns a tuple consisting
 *  of (word,metadata).
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.db.storage.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_iter_words_meta (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    DBC *cursor = NULL;
    DB *field;
    int dbret;
    PyObject *iter = NULL;
    terane_Iter_ops ops = { .next = _Segment_next_word_meta };

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!", &txn, &PyString_Type, &fieldname))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* if the field doesn't exist set KeyError and return */
    field = Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* create a new cursor */
    dbret = field->cursor (field, txn? txn->txn : NULL, &cursor, 0);
    /* if cursor allocation failed, return Error */
    if (dbret != 0)
        return PyErr_Format (terane_Exc_Error, "Failed to allocate DB cursor: %s",
            db_strerror (dbret));
    iter = Iter_new_range ((PyObject *) self, cursor, &ops, (void *)"!", 1);
    if (iter == NULL)
        cursor->close (cursor);
    return iter;
}

/*
 * terane_Segment_iter_words_meta_from: Iterate through all words in the specified
 *  field, starting at the specified word, and return the metadata for each word.
 *
 * callspec: Segment.iter_word_meta_from(txn, fieldname, start)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 *   start (unicode): The word text to start at
 * returns: a new Iterator object.  Each iteration returns a tuple consisting
 *  of (word,metadata).
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.db.storage.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_iter_words_meta_from (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *start = NULL;
    DB *field = NULL;
    DBT *key = NULL;
    DBC *cursor = NULL;
    int dbret;
    PyObject *iter = NULL;
    terane_Iter_ops ops = { .next = _Segment_next_word_meta };

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!", &txn, &PyString_Type, &fieldname,
        &PyUnicode_Type, &start))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* if the field doesn't exist set KeyError and return */
    field = Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the word value */
    key = _Segment_make_meta_key (start);
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
    iter = Iter_new_from ((PyObject *) self, cursor, &ops, key->data, key->size);
    _Segment_free_key (key);
    if (iter == NULL)
        cursor->close (cursor);
    return iter;
}

/*
 * terane_Segment_iter_word_meta_range: Iterate through all words matching the
 *  specified prefix in the specified field and return the metadata for each word.
 *
 * callspec: Segment.iter_word_meta_range(fieldname, prefix)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 *   prefix (unicode): The word prefix to match
 * returns: a new Iterator object.  Each iteration returns a tuple consisting
 *  of (word,metadata).
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.db.storage.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_iter_words_meta_range (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *prefix = NULL;
    DB *field = NULL;
    DBT *key = NULL;
    DBC *cursor = NULL;
    int dbret;
    PyObject *iter = NULL;
    terane_Iter_ops ops = { .next = _Segment_next_word_meta };

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!O!", &txn, &PyString_Type, &fieldname,
        &PyUnicode_Type, &prefix))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* if the field doesn't exist set KeyError and return */
    field = Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* build the key from the word value */
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
    iter = Iter_new_range ((PyObject *) self, cursor, &ops, key->data, key->size - 1);
    if (iter == NULL)
        cursor->close (cursor);
    _Segment_free_key (key);
    return iter;
}
