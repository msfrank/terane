/*
 * Copyright 2012 Michael Frank <msfrank@syntaxjockey.com>
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
 * terane_Segment_get_posting:
 *
 * callspec: Segment.get_posting(txn, posting, **flags)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   posting (object): The posting key
 * returns: The posting value
 * exceptions:
 *   KeyError: The specified record doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_get_posting (terane_Segment *self, PyObject *args, PyObject *kwds)
{
    terane_Txn *txn = NULL;
    PyObject *posting = NULL;
    DBT key, data;
    PyObject *value = NULL;
    int dbflags, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO", &txn, &posting))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");
    if ((dbflags = _terane_parse_db_get_flags (kwds)) < 0)
        return NULL;

    /* build the key */
    memset (&key, 0, sizeof (DBT));
    key.flags = DB_DBT_REALLOC;
    if (_terane_msgpack_dump (posting, (char **) &key.data, &key.size) < 0)
        return NULL;

    /* get the record with the GIL released */
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    Py_BEGIN_ALLOW_THREADS
    dbret = self->postings->get (self->postings, txn? txn->txn : NULL, &key, &data, dbflags);
    Py_END_ALLOW_THREADS
    PyMem_Free (key.data);
    switch (dbret) {
        case 0:
            /* load the data */
            _terane_msgpack_load ((char *) data.data, data.size, &value);
            break;
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            /* document doesn't exist, raise KeyError */
            PyErr_Format (PyExc_KeyError, "Posting doesn't exist");
            break;
        default:
            /* some other db error, raise Error */
            PyErr_Format (terane_Exc_Error, "Failed to get posting: %s",
                db_strerror (dbret));
            break;
    }

    /* free allocated memory */
    if (data.data)
        PyMem_Free (data.data);
    return value;
}

/*
 * terane_Segment_set_posting:
 *
 * callspec: Segment.set_posting(txn, posting, **flags)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   psting (object): The posting key
 *   value (object): The posting value
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_set_posting (terane_Segment *self, PyObject *args, PyObject *kwds)
{
    terane_Txn *txn = NULL;
    PyObject *posting = NULL;
    PyObject *value = NULL;
    DBT key, data;
    int dbflags, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!OO", &terane_TxnType, &txn, &posting, &value))
        return NULL;
    if ((dbflags = _terane_parse_db_put_flags (kwds)) < 0)
        return NULL;

    /* build the key */
    memset (&key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (posting, (char **) &key.data, &key.size) < 0)
        return NULL;
    /* build the value */
    memset (&data, 0, sizeof (DBT));
    if (_terane_msgpack_dump (value, (char **) &data.data, &data.size) < 0) {
        PyMem_Free (key.data);
        return NULL;
    }
    /* set the record with the GIL released */
    Py_BEGIN_ALLOW_THREADS
    dbret = self->postings->put (self->postings, txn->txn, &key, &data, dbflags);
    Py_END_ALLOW_THREADS
    PyMem_Free (key.data);
    PyMem_Free (data.data);
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
 * terane_Segment_contains_posting: Determine whether the specified posting
 *  exists.
 *
 * callspec: Segment.contains_posting(txn, posting, **flags)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   posting (object): The posting key.
 * returns: True if the posting exists, otherwise False.
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to find the record
 */
PyObject *
terane_Segment_contains_posting (terane_Segment *self, PyObject *args, PyObject *kwds)
{
    terane_Txn *txn = NULL;
    PyObject *posting = NULL;
    DBT key;
    int dbflags, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO", &txn, &posting))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");
    if ((dbflags = _terane_parse_db_exists_flags (kwds)) < 0)
        return NULL;

    /* build the key */
    memset (&key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (posting, (char **) &key.data, &key.size) < 0)
        return NULL;
    key.flags = DB_DBT_REALLOC;

    /* check for the record with the GIL released */
    Py_BEGIN_ALLOW_THREADS
    dbret = self->postings->exists (self->postings, txn? txn->txn : NULL, &key, dbflags);
    Py_END_ALLOW_THREADS
    PyMem_Free (key.data);
    switch (dbret) {
        case 0:
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            break;
        default:
            /* some other db error, raise Exception */
            return PyErr_Format (terane_Exc_Error, "Failed to find term: %s",
                db_strerror (dbret));
    }
    if (dbret == 0)
        Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

/*
 * terane_Segment_estimate_postings: Return an estimate of the
 *  number of postings between the specified start and end postings.
 *
 * callspec: Segment.estimate_postings(txn, start, end)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   start (object): The starting posting
 *   end (object): The ending posting
 * returns: An estimate of the percentage of total postings which
 *   are between the start and end postings, expressed as a float.
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to find the record
 */
PyObject *
terane_Segment_estimate_postings (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *start = NULL, *end = NULL, *estimate = NULL;
    DBT start_key, end_key;
    DB_KEY_RANGE start_range, end_range;
    int dbret, cmp;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OOO", &txn, &start, &end))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* build the start and end keys */
    memset (&start_key, 0, sizeof (DBT));
    memset (&end_key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (start, (char **) &start_key.data, &start_key.size) < 0)
        goto error;
    if (_terane_msgpack_dump (end, (char **) &end_key.data, &end_key.size) < 0)
        goto error;

    /* estimate start key range */
    dbret = self->postings->key_range (self->postings, txn? txn->txn : NULL,
        &start_key, &start_range, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to estimate start key range: %s",
            db_strerror (dbret));
        goto error;
    }
    /* estimate start key range */
    dbret = self->postings->key_range (self->postings, txn? txn->txn : NULL,
         &end_key, &end_range, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to estimate end key range: %s",
            db_strerror (dbret));
        goto error;
    }

    dbret = _terane_msgpack_cmp ((char *) start_key.data, start_key.size,
        (char *) end_key.data, end_key.size, &cmp);
    if (dbret == 0) {
        if (cmp > 0)
            estimate = PyFloat_FromDouble (1.0 - (end_range.less + start_range.greater));
        else
            estimate = PyFloat_FromDouble (1.0 - (start_range.less + end_range.greater));
    }

error:
    if (start_key.data)
        PyMem_Free (start_key.data);
    if (end_key.data)
        PyMem_Free (end_key.data);
    return estimate;
}

/*
 * terane_Segment_iter_postings: Iterate through all postings associated
 *  with the specified term in the specified field.
 *
 * callspec: Segment.iter_postings(txn, start, end, **flags)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   start (object): The posting key marking the start of the range
 *   end (object): The posting key marking the end of the range
 *   reverse (bool): If True, then terate in reverse
 * returns: a new Iterator object.  Each iteration returns a tuple consisting
 *  of (key,value).
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_iter_postings (terane_Segment *self, PyObject *args, PyObject *kwds)
{
    terane_Txn *txn = NULL;
    PyObject *start = NULL, *end = NULL, *reverse = NULL;
    DBC *cursor = NULL;
    int dbflags, dbret;
    PyObject *iter = NULL;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OOOO", &txn, &start, &end, &reverse))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");
    if (reverse != Py_True && reverse != Py_False)
        return PyErr_Format (PyExc_TypeError, "reverse must be True or False");
    if ((dbflags = _terane_parse_db_cursor_flags (kwds)) < 0)
        return NULL;

    /* create a new cursor with the GIL released */
    Py_BEGIN_ALLOW_THREADS
    dbret = self->postings->cursor (self->postings, txn? txn->txn : NULL, &cursor, dbflags);
    Py_END_ALLOW_THREADS
    if (dbret != 0)
        return PyErr_Format (terane_Exc_Error, "Failed to allocate DB cursor: %s",
            db_strerror (dbret));
    if (start == Py_None && end == Py_None)
        iter = terane_Iter_new ((PyObject *) self, cursor,
            reverse == Py_True ? 1 : 0);
    else if (end == Py_None)
        iter = terane_Iter_new_from ((PyObject *) self, cursor,
            start, reverse == Py_True ? 1 : 0);
    else if (start == Py_None)
        iter = terane_Iter_new_until ((PyObject *) self, cursor,
            end, reverse == Py_True ? 1 : 0);
    else
        iter = terane_Iter_new_within ((PyObject *) self, cursor,
            start, end, reverse == Py_True ? 1 : 0);
    if (iter == NULL) {
        Py_BEGIN_ALLOW_THREADS
        cursor->close (cursor);
        Py_END_ALLOW_THREADS
    }
    return iter;
}
