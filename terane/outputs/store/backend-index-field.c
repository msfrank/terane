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
 * terane_Index_get_field: get the pickled fieldspec for the field.
 *
 * callspec: Index.get_field(txn, fieldname)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 * returns: The field specification.
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the field
 */
PyObject *
terane_Index_get_field (terane_Index *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    DBT key, data;
    PyObject *fieldspec = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO", &txn, &fieldname))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* use the fieldname as key */
    memset (&key, 0, sizeof (DBT));
    key.flags = DB_DBT_REALLOC;
    if (_terane_msgpack_dump (fieldname, (char **) &key.data, &key.size) < 0)
        return NULL;
    /* get the record */
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    dbret = self->schema->get (self->schema, txn? txn->txn : NULL, &key, &data, 0);
    switch (dbret) {
        case 0:
            /* deserialize the data */
            _terane_msgpack_load (data.data, data.size, &fieldspec);
            break;
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            /* metadata doesn't exist, raise KeyError */
            PyErr_Format (PyExc_KeyError, "Field %s doesn't exist",
                (char *) key.data);
            break;
        default:
            /* some other db error, raise Exception */
            PyErr_Format (terane_Exc_Error, "Failed to get field %s: %s",
                (char *) key.data, db_strerror (dbret));
            break;
    }

    /* free allocated memory */
    if (key.data)
        PyMem_Free (key.data);
    if (data.data)
        PyMem_Free (data.data);
    return fieldspec;
}

/*
 * terane_Index_add_field: add the field to the Store.
 *
 * callspec: Index.add_field(txn, fieldname, fieldspec)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   fieldname (string): The field name
 *   fieldspec (object): The field specification.
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to add the field
 */
PyObject *
terane_Index_add_field (terane_Index *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *fieldspec = NULL;
    DBT key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!OO", &terane_TxnType, &txn,
        &fieldname, &fieldspec))
        return NULL;

    /* use the fieldname as the key */
    memset (&key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (fieldname, (char **) &key.data, &key.size) < 0)
        return NULL;
    memset (&data, 0, sizeof (DBT));
    if (_terane_msgpack_dump (fieldspec, (char **) &data.data, &data.size) < 0) {
        PyMem_Free (key.data);
        return NULL;
    }
    /* set the record */
    dbret = self->schema->put (self->schema, txn->txn, &key, &data, DB_NOOVERWRITE);
    switch (dbret) {
        case 0:
            /* increment the internal field count */
            self->nfields += 1;
            break;
        case DB_KEYEXIST:
            PyErr_Format (PyExc_KeyError, "Field already exists");
            break;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to set fieldspec: %s",
                db_strerror (dbret));
            break;
    }
    if (key.data)
        PyMem_Free (key.data);
    if (data.data)
        PyMem_Free (data.data);
    Py_RETURN_NONE;
}

/*
 * terane_Index_contains_field: return True if field exists in the schema
 *
 * callspec: Index.contains_field(txn, fieldname)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 * returns: True if the field exists, otherwise False.
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the fields
 */
PyObject *
terane_Index_contains_field (terane_Index *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    DBT key;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO", &txn, &fieldname))
        return NULL;
    if ((PyObject *)txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* see if fieldname exists in the schema */
    memset (&key, 0, sizeof (key));
    if (_terane_msgpack_dump (fieldname, (char **) &key.data, &key.size) < 0)
        return NULL;
    dbret = self->schema->exists (self->schema, txn? txn->txn : NULL, &key, 0);
    PyMem_Free (key.data);
    switch (dbret) {
        case 0:
            Py_RETURN_TRUE;
        case DB_NOTFOUND:
            Py_RETURN_FALSE;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to lookup field in schema: %s",
                db_strerror (dbret));
            break;
    }
    return NULL;
}

/*
 * _Index_next_field: build a (fieldname,fieldspec) tuple
 */
static PyObject *
_Index_next_field (terane_Iter *iter, DBT *key, DBT *data)
{
    PyObject *fieldname = NULL, *fieldspec = NULL, *tuple = NULL;

    /* get the posting */
    if (_terane_msgpack_load ((char *) key->data, key->size, &fieldname) < 0)
        goto error;
    /* get the value */
    if (_terane_msgpack_load ((char *) data->data, data->size, &fieldspec) < 0)
        goto error;
    /* build the (posting,value) tuple */
    tuple = PyTuple_Pack (2, fieldname, fieldspec);
error:
    Py_XDECREF (fieldname);
    Py_XDECREF (fieldspec);
    return tuple;
}

/*
 * terane_Index_iter_fields: iterate through the fields in the schema.
 *
 * callspec: Index.iter_fields(txn)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 * returns: a list of (fieldname,fieldspec) tuples.
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the fields
 */
PyObject *
terane_Index_iter_fields (terane_Index *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    DBC *cursor = NULL;
    PyObject *iter = NULL;
    terane_Iter_ops ops = { .next = _Index_next_field };
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O", &txn))
        return NULL;
    if ((PyObject *)txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* create a new cursor */
    dbret = self->schema->cursor (self->schema, txn? txn->txn : NULL, &cursor, 0);
    /* if cursor allocation failed, return Error */
    if (dbret != 0)
        return PyErr_Format (terane_Exc_Error, "Failed to allocate document cursor: %s",
            db_strerror (dbret));

    /* allocate a new Iter object */
    iter = terane_Iter_new ((PyObject *) self, cursor, &ops, 0);
    if (iter == NULL) {
        cursor->close (cursor);
        return NULL;
    }
    return iter;
}

/*
 * terane_Index_count_fields: Return the number of fields in the schema.
 *
 * callspec: Index.count_fields()
 * parameters: None
 * returns: The number of fields in the schema. 
 * exceptions: None
 */
PyObject *
terane_Index_count_fields (terane_Index *self)
{
    return PyLong_FromUnsignedLong (self->nfields);
}
