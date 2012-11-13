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
 * terane_Segment_get_field: Retrieve the value associated with the
 *  specified field.
 *
 * callspec: Segment.get_field(txn, field)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   field (object): The field (name,type) tuple
 * returns: The field value.
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_get_field (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *field = NULL;
    DBT key, data;
    PyObject *value = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO", &txn, &field))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    /* use the field as the key */
    memset (&key, 0, sizeof (DBT));
    key.flags = DB_DBT_REALLOC;
    if (_terane_msgpack_dump (field, (char **) &key.data, &key.size) < 0)
        return NULL;
    /* get the record */
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    dbret = self->fields->get (self->fields, txn? txn->txn : NULL, &key, &data, 0);
    switch (dbret) {
        case 0:
            /* create a python string from the data */
            _terane_msgpack_load ((char *) data.data, data.size, &value);
            break;
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            /* metadata doesn't exist, raise KeyError */
            PyErr_Format (PyExc_KeyError, "Metadata doesn't exist");
            break;
        default:
            /* some other db error, raise Error */
            PyErr_Format (terane_Exc_Error, "Failed to get metadata for field: %s",
                db_strerror (dbret));
            break;
    }
    /* free allocated memory */
    if (key.data)
        PyMem_Free (key.data);
    if (data.data)
        PyMem_Free (data.data);
    return value;
}

/*
 * terane_Segment_set_field: Change the value associated with the
 *  specified field.
 *
 * callspec: Segment.set_field(txn, field, value)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   field (object): The field (name,type) tuple
 *   value (object): The field value
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_set_field (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *field = NULL;
    PyObject *value = NULL;
    DBT key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!OO", &terane_TxnType, &txn, &field, &value))
        return NULL;
    /* use the field as the key */
    memset (&key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (field, (char **) &key.data, &key.size) < 0)
        return NULL;
    /* set the field */
    memset (&data, 0, sizeof (DBT));
    if (_terane_msgpack_dump (value, (char **) &data.data, &data.size) < 0) {
        PyMem_Free (key.data);
        return NULL;
    }
    /* set the record */
    dbret = self->fields->put (self->fields, txn->txn, &key, &data, 0);
    PyMem_Free (key.data);
    PyMem_Free (data.data);
    switch (dbret) {
        case 0:
            break;
        default:
            /* some other db error, raise Error */
            return PyErr_Format (terane_Exc_Error, "Failed to set metadata for field: %s",
                db_strerror (dbret));
    }
    Py_RETURN_NONE;
}
