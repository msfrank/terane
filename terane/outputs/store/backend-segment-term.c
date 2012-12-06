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
 * terane_Segment_get_term: Retrieve the value associated with the
 *  specified term in the specified field.
 *
 * callspec: Segment.get_term(txn, term)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   term (object): The term key
 * returns: The term value
 * exceptions:
 *   KeyError: The specified field or metadata doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_get_term (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *term = NULL;
    DBT key, data;
    PyObject *value = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO", &txn, &term))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* build the key from the fieldname and term values */
    memset (&key, 0, sizeof (DBT));
    key.flags = DB_DBT_REALLOC;
    if (_terane_msgpack_dump (term, (char **) &key.data, &key.size) < 0)
        return NULL;

    /* get the record */
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    dbret = self->terms->get (self->terms, txn? txn->txn : NULL, &key, &data, 0);
    PyMem_Free (key.data);
    switch (dbret) {
        case 0:
            /* create a python string from the data */
            _terane_msgpack_load ((char *) data.data, data.size, &value);
            break;
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            /* document doesn't exist, raise KeyError */
            PyErr_Format (PyExc_KeyError, "Metadata doesn't exist");
            break;
        default:
            /* some other db error, raise Error */
            PyErr_Format (terane_Exc_Error, "Failed to get metadata: %s",
                db_strerror (dbret));
            break;
    }
    
    /* free allocated memory */
    if (data.data)
        PyMem_Free (data.data);
    return value;
}

/*
 * terane_Segment_set_term: Change the value associated with the
 *  specified term in the specified field.
 *
 * callspec: Segment.set_term(txn, term, value)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   term (object): The term key
 *   value (object): The term value
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_set_term (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *term = NULL;
    PyObject *value = NULL;
    DBT key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!OO", &terane_TxnType, &txn, &term, &value))
        return NULL;

    /* build the key from the fieldname and term value */
    memset (&key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (term, (char **) &key.data, &key.size) < 0)
        return NULL;

    memset (&data, 0, sizeof (DBT));
    if (_terane_msgpack_dump (value, (char **) &data.data, &data.size) < 0) {
        PyMem_Free (key.data);
        return NULL;
    }

    /* set the record */
    dbret = self->terms->put (self->terms, txn->txn, &key, &data, 0);
    PyMem_Free (key.data);
    PyMem_Free (data.data);
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
 * _Segment_next_term: return the (term,value) tuple from the current cursor item
 */
static PyObject *
_Segment_next_term (terane_Iter *iter, DBT *key, DBT *data)
{
    PyObject *term = NULL, *value = NULL, *tuple = NULL;

    /* get the posting */
    if (_terane_msgpack_load ((char *) key->data, key->size, &term) < 0)
        goto error;
    /* get the value */
    if (_terane_msgpack_load ((char *) data->data, data->size, &value) < 0)
        goto error;
    /* build the (posting,value) tuple */
    tuple = PyTuple_Pack (2, term, value);
error:
    Py_XDECREF (term);
    Py_XDECREF (value);
    return tuple;
}

/*
 * terane_Segment_iter_terms: Iterate through all terms in the specified field.
 *
 * callspec: Segment.iter_terms(txn, start, end)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   start (object): The term key marking the start of the range
 *   end (object): The term key marking the end of the range
 * returns: a new Iterator object.  Each iteration returns a tuple consisting
 *  of (key,value).
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_iter_terms (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *start = NULL, *end = NULL;
    DBC *cursor = NULL;
    int dbret;
    PyObject *iter = NULL;
    terane_Iter_ops ops = { .next = _Segment_next_term };

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OOO", &txn, &start, &end))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");
    if (start == Py_None && end == Py_None)
        return PyErr_Format (PyExc_TypeError, "start and end are None");

    /* create a new cursor */
    dbret = self->terms->cursor (self->terms, txn? txn->txn : NULL, &cursor, 0);
    if (dbret != 0)
        return PyErr_Format (terane_Exc_Error, "Failed to allocate DB cursor: %s",
            db_strerror (dbret));

    /* create the Iter */
    if (end == Py_None)
        iter = terane_Iter_new_prefix ((PyObject *) self, cursor, &ops, start, 0);
    else if (start == Py_None)
        iter = terane_Iter_new_prefix ((PyObject *) self, cursor, &ops, end, 1);
    else
        iter = terane_Iter_new_within ((PyObject *) self, cursor, &ops, start, end, 0);
    if (iter == NULL) 
        cursor->close (cursor);
    return iter;
}
