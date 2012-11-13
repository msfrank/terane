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
 * terane_Index_list_fields: return a list of the fields in the schema
 *
 * callspec: Index.list_fields(txn)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 * returns: a list of (fieldname,pickledfield) tuples.
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the fields
 */
PyObject *
terane_Index_list_fields (terane_Index *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    DBC *cursor = NULL;
    PyObject *fields = NULL, *tuple = NULL, *fieldname = NULL, *fieldspec = NULL;
    DBT key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O", &txn))
        return NULL;
    if ((PyObject *)txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* allocate the list to return */
    fields = PyList_New (0);
    if (fields == NULL)
        return PyErr_NoMemory ();

    /* */
    dbret = self->schema->cursor (self->schema, txn? txn->txn : NULL, &cursor, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open schema cursor: %s",
            db_strerror (dbret));
        goto error;
    }

    /* loop through each schema item */
    while (1) {
        memset (&key, 0, sizeof (DBT));
        key.flags = DB_DBT_MALLOC;
        memset (&data, 0, sizeof (DBT));
        data.flags = DB_DBT_MALLOC;
        dbret = cursor->get (cursor, &key, &data, DB_NEXT);
        if (dbret == DB_NOTFOUND)
            break;
        switch (dbret) {
            case 0:
                fieldname = PyString_FromStringAndSize (key.data, key.size - 1);
                if (fieldname == NULL)
                    goto error;
                if (_terane_msgpack_load (data.data, data.size, &fieldspec) < 0)
                    goto error;
                tuple = PyTuple_Pack (2, fieldname, fieldspec);
                if (tuple == NULL)
                    goto error;
                if (PyList_Append (fields, tuple) < 0)
                    goto error;
                Py_DECREF (fieldname);
                fieldname = NULL;
                Py_DECREF (fieldspec);
                fieldspec = NULL;
                Py_DECREF (tuple);
                tuple = NULL;
                if (key.data)
                    PyMem_Free (key.data);
                key.data = NULL;
                if (data.data)
                    PyMem_Free (data.data);
                data.data = NULL;
                break;
            default:
                PyErr_Format (terane_Exc_Error, "Failed to get next schema field: %s",
                    db_strerror (dbret));
                goto error;
        }
    }

    cursor->close (cursor);
    return fields;

/* if there is an error, then free any locally allocated memory and references */
error:
    if (cursor != NULL)
        cursor->close (cursor);
    if (fieldname != NULL)
        Py_DECREF (fieldname);
    if (fieldspec != NULL)
        Py_DECREF (fieldspec);
    if (tuple != NULL)
        Py_DECREF (tuple);
    if (fields != NULL)
        Py_DECREF (fields);
    if (key.data != NULL)
        PyMem_Free (key.data);
    if (data.data != NULL)
        PyMem_Free (data.data);
    return NULL;
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
