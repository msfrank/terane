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
 * terane_Index_get_meta: get a Index metadata value
 *
 * callspec: Index.get_meta(txn, id)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   id (string): The metadata id
 * returns: A string representing the metadata value 
 * exceptions:
 *   KeyError: The document with the specified id doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to retrieve the record
 */
PyObject *
terane_Index_get_meta (terane_Index *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    const char *id = NULL;
    DBT key, data;
    PyObject *metadata = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "Os", &txn, &id))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* use the document id as the record number */
    memset (&key, 0, sizeof (DBT));
    key.data = (char *) id;
    key.size = strlen(id) + 1;
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;

    /* get the record */
    dbret = self->metadata->get (self->metadata, txn? txn->txn : NULL, &key, &data, 0);
    switch (dbret) {
        case 0:
            /* create a python string from the data */
            metadata = PyString_FromString ((char *) data.data);
            break;
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            /* metadata doesn't exist, raise KeyError */
            PyErr_Format (PyExc_KeyError, "Metadata id %s doesn't exist",
                (char *) key.data);
            break;
        default:
            /* some other db error, raise Exception */
            PyErr_Format (terane_Exc_Error, "Failed to get metadata %s: %s",
                (char *) key.data, db_strerror (dbret));
            break;
    }

    /* free allocated memory */
    if (data.data)
        PyMem_Free (data.data);
    return metadata;
}

/*
 * terane_Index_set_meta: set a Index metadata value
 *
 * callspec: Index.set_meta(txn, id, value)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   id (string): The metadata id
 *   value (string): Metadata to store
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Index_set_meta (terane_Index *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    const char *id = NULL;
    const char *metadata = NULL;
    DBT key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!ss", &terane_TxnType, &txn, &id, &metadata))
        return NULL;

    memset (&key, 0, sizeof (DBT));
    memset (&data, 0, sizeof (DBT));
    key.data = (char *) id;
    key.size = strlen (id) + 1;
    data.data = (char *) metadata;
    data.size = strlen(metadata) + 1;
    /* set the record */
    dbret = self->metadata->put (self->metadata, txn->txn, &key, &data, 0);
    /* db error, raise Exception */
    switch (dbret) {
        case 0:
            break;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to set metadata %s: %s",
                (char *) key.data, db_strerror (dbret));
            break;
    }
    Py_RETURN_NONE;
}
