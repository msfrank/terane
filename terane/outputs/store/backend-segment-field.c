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
 * _Segment_make_field_meta_key:
 */
static DBT *
_Segment_make_field_meta_key (PyObject *fieldname)
{
    char *field_str;
    Py_ssize_t field_len;
    DBT *key = NULL;

    assert (term != NULL);

    /* convert term from UTF-16 to UTF-8 */
    if (PyString_AsStringAndSize (fieldname, &field_str, &field_len) < 0)
        return NULL;        /* raises TypeError */

    /* allocate a DBT to store the key */
    key = PyMem_Malloc (sizeof (DBT));
    if (key == NULL) {
        PyErr_NoMemory ();
        return NULL;    /* raises MemoryError */
    }
    memset (key, 0, sizeof (DBT));

    /* create key in the form of '!' + fieldname + '\0' */
    key->data = PyMem_Malloc (field_len + 2);
    if (key->data == NULL) {
        PyErr_NoMemory ();
        PyMem_Free (key);
        return NULL;    /* raises MemoryError */
    }
    key->size = field_len + 2;
    ((char *)key->data)[0] = '!';
    memcpy (key->data + 1, field_str, field_len);
    ((char *)key->data)[field_len + 1] = '\0';

    /* we need to set USERMEM because the postings db is opened with DB_THREAD */
    key->flags = DB_DBT_USERMEM;
    key->ulen = key->size;

    return key;
}

/*
 * _Segment_free_field_key:
 */
static void
_Segment_free_field_key (DBT *key)
{
    assert (key != NULL);
    if (key->data != NULL)
        PyMem_Free (key->data);
    PyMem_Free (key);
}

/*
 * terane_Segment_get_field_meta: Retrieve the metadata associated with the
 *  specified field.
 *
 * callspec: Segment.get_field_meta(txn, fieldname)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 * returns: a string containing the JSON-encoded metadata. 
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_get_field_meta (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    DBT *key, data;
    PyObject *metadata = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!", &txn, &PyString_Type, &fieldname))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;

    /* get the record */
    key = _Segment_make_field_meta_key (fieldname);
    if (key == NULL)
        return NULL;
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    dbret = self->postings->get (self->postings, txn? txn->txn : NULL, key, &data, 0);
    _Segment_free_field_key (key);
    switch (dbret) {
        case 0:
            /* create a python string from the data */
            metadata = PyString_FromString (data.data);
            break;
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            /* metadata doesn't exist, raise KeyError */
            return PyErr_Format (PyExc_KeyError, "Metadata for %s doesn't exist",
                PyString_AsString (fieldname));
            break;
        default:
            /* some other db error, raise Error */
            return PyErr_Format (terane_Exc_Error, "Failed to get metadata for field %s: %s",
                PyString_AsString (fieldname), db_strerror (dbret));
    }

    /* free allocated memory */
    if (data.data)
        PyMem_Free (data.data);
    return metadata;
}

/*
 * terane_Segment_set_field_meta: Change the metadata associated with the
 *  specified field.
 *
 * callspec: Segment.set_field_meta(fieldname, metadata)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   fieldname (string): The field name
 *   metadata (string): The JSON-encoded metadata
 * returns: None
 * exceptions:
 *   KeyError: The specified field doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_set_field_meta (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    const char *metadata = NULL;
    DBT *key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!O!s", &terane_TxnType, &txn,
        &PyString_Type, &fieldname, &metadata))
        return NULL;

    /* set the record */
    key = _Segment_make_field_meta_key (fieldname);
    if (key == NULL)
        return NULL;
    memset (&data, 0, sizeof (DBT));
    data.data = (char *) metadata;
    data.size = strlen (metadata) + 1;
    dbret = self->postings->put (self->postings, txn->txn, key, &data, 0);
    _Segment_free_field_key (key);
    switch (dbret) {
        case 0:
            break;
        default:
            /* some other db error, raise Error */
            return PyErr_Format (terane_Exc_Error, "Failed to set metadata for field %s: %s",
                PyString_AsString (fieldname), db_strerror (dbret));
    }

    Py_RETURN_NONE;
}
