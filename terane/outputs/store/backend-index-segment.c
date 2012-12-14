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
 * terane_Index_add_segment: add a new segment to the index.
 *
 * callspec: Index.add_segment(txn, name, value)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   id (object): The segment id
 *   value (object): The segment metadata to store
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to add the record.
 */
PyObject *
terane_Index_add_segment (terane_Index *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *id = NULL;
    PyObject *metadata = NULL;
    DBT key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!OO", &terane_TxnType, &txn, &id, &metadata))
        return NULL;
    /*  use the segment name as the key */
    memset (&key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (id, (char **) &key.data, &key.size) < 0)
        return NULL;
    /* serialize the metadata */
    memset (&data, 0, sizeof (DBT));
    if (_terane_msgpack_dump (metadata, (char **) &data.data, &data.size) < 0) {
        PyMem_Free (key.data);
        return NULL;
    }
    /* add the record */
    dbret = self->segments->put (self->segments, txn->txn, &key, &data, DB_NOOVERWRITE);
    if (key.data)
        PyMem_Free (key.data);
    if (data.data)
        PyMem_Free (data.data);
    switch (dbret) {
        case 0:
            break;
        default:
            return PyErr_Format (terane_Exc_Error, "Failed to add segment: %s",
                db_strerror (dbret));
    }
    Py_RETURN_NONE;
}

/*
 * terane_Index_iter_segments: iterate through all segments.
 *
 * callspec: Index.iter_segments(txn)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 * returns: a new Iter object.  Each iteration returns a tuple consisting of
 *  (id, metadata).
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to create the iterator
 */
PyObject *
terane_Index_iter_segments (terane_Index *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    DBC *cursor = NULL;
    PyObject *iter = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O", &txn))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");
    
    /* create a new cursor */
    dbret = self->segments->cursor (self->segments, txn? txn->txn : NULL, &cursor, 0);
    /* if cursor allocation failed, return Error */
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to allocate segment cursor: %s",
            db_strerror (dbret));
        return NULL;
    }
    iter = terane_Iter_new ((PyObject *) self, cursor, 0);
    if (iter == NULL)
        cursor->close (cursor);
    return iter;
}

/*
 * terane_Index_delete_segment: remove the segment from the Index.
 *
 * callspec: Index.delete_segment(txn, segment)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   id (object): The id of the segment to delete
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying count the segments
 */
PyObject *
terane_Index_delete_segment (terane_Index *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *id = NULL;
    DBT key;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!O", &terane_TxnType, &txn, &id))
        return NULL;

    /* delete segment id from the Index */
    memset (&key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (id, (char **) &key.data, &key.size) < 0)
        return NULL;
    dbret = self->segments->del (self->segments, txn->txn, &key, 0);
    if (key.data)
        PyMem_Free (key.data);
    switch (dbret) {
        case 0:
            break;
        default:
            return PyErr_Format (terane_Exc_Error, "Failed to delete segment: %s",
                db_strerror (dbret));
    }
    Py_RETURN_NONE;
}
