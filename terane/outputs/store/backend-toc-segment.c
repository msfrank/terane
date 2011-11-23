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
 * terane_TOC_new_segment: allocate a new Segment id.
 *
 * callspec: TOC.new_segment(txn)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 * returns: A long representing the segment id.
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to allocate the segment.
 */
PyObject *
terane_TOC_new_segment (terane_TOC *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    db_recno_t sid = 0;
    DBT key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!", &terane_TxnType, &txn))
        return NULL;

    /* allocate a new segment record */
    memset (&key, 0, sizeof (DBT));
    key.flags = DB_DBT_MALLOC;
    memset (&data, 0, sizeof (DBT));
    dbret = self->schema->put (self->segments, txn->txn, &key, &data, DB_APPEND);
    switch (dbret) {
        case 0:
            break;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to allocate new segment: %s",
                db_strerror (dbret));
            return NULL;
    }
    /* increment the internal segment count */
    self->nsegments += 1;
    /* return the segment record number */
    sid = *((db_recno_t *) key.data);
    /* free allocated memory */
    if (key.data)
        PyMem_Free (key.data);
    return PyLong_FromUnsignedLong ((unsigned long) sid);
}

/*
 * TOC_contains_segment: return true if segment exists in the TOC
 */
int
terane_TOC_contains_segment (terane_TOC *self, terane_Txn *txn, db_recno_t sid)
{
    DBT key;
    int dbret;

    memset (&key, 0, sizeof (key));
    key.data = &sid;
    key.size = sizeof (sid);
    dbret = self->schema->exists (self->segments, txn? txn->txn : NULL, &key, 0);
    switch (dbret) {
        case 0:
            return 1;
        case DB_NOTFOUND:
            return 0;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to lookup segment %lu in segments: %s",
                (unsigned long int) sid, db_strerror (dbret));
            break;
    }
    return -1;
}

/*
 * _TOC_next_segment: return the segment id from the current cursor item
 */
static PyObject *
_TOC_next_segment (terane_Iter *iter, DBT *key, DBT *data)
{
    db_recno_t sid = 0;

    sid = *((db_recno_t *) key->data);
    return PyLong_FromUnsignedLong ((unsigned long) sid);
}

/*
 * terane_TOC_iter_segments: iterate through all segments.
 *
 * callspec: TOC.iter_segments(txn)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 * returns: a new Iter object.  Each iteration returns a long representing the
 *  segment id.
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to create the iterator
 */
PyObject *
terane_TOC_iter_segments (terane_TOC *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    DBC *cursor = NULL;
    PyObject *iter = NULL;
    terane_Iter_ops ops = { .next = _TOC_next_segment };
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
    iter = terane_Iter_new ((PyObject *) self, cursor, &ops);
    if (iter == NULL)
        cursor->close (cursor);
    return iter;
}

/*
 * terane_TOC_count_segments: return the number of segments in the TOC.
 *
 * callspec: TOC.count_segments()
 * parameters: None
 * returns: The number of segments in the TOC.
 * exceptions: None
 */
PyObject *
terane_TOC_count_segments (terane_TOC *self)
{
    return PyLong_FromUnsignedLong (self->nsegments);
}

/*
 * terane_TOC_delete_segment: remove the segment from the TOC.
 *
 * callspec: TOC.delete_segment(txn, segment)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   id (long): The identifier of the Segment to delete
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying count the segments
 */
PyObject *
terane_TOC_delete_segment (terane_TOC *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    unsigned long sid= 0;
    DBT key;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!k", &terane_TxnType, &txn, &sid))
        return NULL;

    /* delete segment id from the TOC */
    memset (&key, 0, sizeof (DBT));
    key.data = &sid;
    key.size = sizeof (unsigned long);
    dbret = self->segments->del (self->segments, txn->txn, &key, 0);
    switch (dbret) {
        case 0:
            break;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to delete segment: %s",
                db_strerror (dbret));
            break;
    }
    /* decrement the internal segment count */
    self->nsegments -= 1;
    Py_RETURN_NONE;
}
