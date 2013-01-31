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
 * terane_Index_get_field: get the value associated with the field.
 *
 * callspec: Index.get_field(txn, fieldname, **flags)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 * returns: The field specification.
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the field
 */
PyObject *
terane_Index_get_field (terane_Index *self, PyObject *args, PyObject *kwds)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    DBT key, data;
    PyObject *fieldspec = NULL;
    int dbflags, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO", &txn, &fieldname))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");
    if ((dbflags = _terane_parse_db_get_flags (kwds)) < 0)
        return NULL;

    /* use the fieldname as key */
    memset (&key, 0, sizeof (DBT));
    key.flags = DB_DBT_REALLOC;
    if (_terane_msgpack_dump (fieldname, (char **) &key.data, &key.size) < 0)
        return NULL;
    /* get the record with the GIL released */
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    Py_BEGIN_ALLOW_THREADS
    dbret = self->schema->get (self->schema, txn? txn->txn : NULL, &key, &data, dbflags);
    Py_END_ALLOW_THREADS
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
 * terane_Index_set_field: Change the value associated with the specified
 *  field.
 *
 * callspec: Index.set_field(txn, fieldname, fieldspec, **flags)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   fieldname (string): The field name
 *   fieldspec (object): The field specification.
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to add the field
 */
PyObject *
terane_Index_set_field (terane_Index *self, PyObject *args, PyObject *kwds)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    PyObject *fieldspec = NULL;
    DBT key, data;
    int dbflags, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!OO", &terane_TxnType, &txn,
        &fieldname, &fieldspec))
        return NULL;
    if ((dbflags = _terane_parse_db_put_flags (kwds)) < 0)
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
    /* set the record with the GIL released */
    Py_BEGIN_ALLOW_THREADS
    dbret = self->schema->put (self->schema, txn->txn, &key, &data, dbflags);
    Py_END_ALLOW_THREADS
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
terane_Index_contains_field (terane_Index *self, PyObject *args, PyObject *kwds)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    DBT key;
    int dbflags, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO", &txn, &fieldname))
        return NULL;
    if ((PyObject *)txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");
    if ((dbflags = _terane_parse_db_exists_flags (kwds)) < 0)
        return NULL;

    /* use the fieldname as the key */
    memset (&key, 0, sizeof (key));
    if (_terane_msgpack_dump (fieldname, (char **) &key.data, &key.size) < 0)
        return NULL;

    /* check for the record with the GIL released */
    Py_BEGIN_ALLOW_THREADS
    dbret = self->schema->exists (self->schema, txn? txn->txn : NULL, &key, dbflags);
    Py_END_ALLOW_THREADS
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
terane_Index_iter_fields (terane_Index *self, PyObject *args, PyObject *kwds)
{
    terane_Txn *txn = NULL;
    DBC *cursor = NULL;
    PyObject *iter = NULL;
    int dbflags, dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O", &txn))
        return NULL;
    if ((PyObject *)txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");
    if ((dbflags = _terane_parse_db_cursor_flags (kwds)) < 0)
        return NULL;

    /* create a new cursor with the GIL released */
    Py_BEGIN_ALLOW_THREADS
    dbret = self->schema->cursor (self->schema, txn? txn->txn : NULL, &cursor, dbflags);
    Py_END_ALLOW_THREADS

    /* if cursor allocation failed, return Error */
    if (dbret != 0)
        return PyErr_Format (terane_Exc_Error, "Failed to allocate document cursor: %s",
            db_strerror (dbret));

    /* allocate a new Iter object */
    iter = terane_Iter_new ((PyObject *) self, cursor, 0);
    if (iter == NULL) {
        Py_BEGIN_ALLOW_THREADS
        cursor->close (cursor);
        Py_END_ALLOW_THREADS
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
