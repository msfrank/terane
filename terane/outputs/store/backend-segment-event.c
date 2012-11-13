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
 * terane_Segment_new_event: create a new event
 *
 * callspec: Segment.new_event(txn, evid)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   evid (object): The event identifier
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.DocExists: The event specified by evid already exists
 *   terane.outputs.store.backend.Error: A db error occurred when trying to create the record
 */
PyObject *
terane_Segment_new_event (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *evid = NULL;
    DBT key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!O", &terane_TxnType, &txn, &evid))
        return NULL;
    /* use the document id as the key */
    memset (&key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (evid, (char **) &key.data, &key.size) < 0)
        return NULL;
    memset (&data, 0, sizeof (DBT));
    /* put a new document.  raise DocExists if the event identifier already exists. */
    dbret = self->events->put (self->events, txn->txn, &key, &data, DB_NOOVERWRITE);
    PyMem_Free (key.data);
    switch (dbret) {
        case 0:
            break;
        case DB_KEYEXIST:
            return PyErr_Format (terane_Exc_DocExists, "Event already exists");
        default:
            return PyErr_Format (terane_Exc_Error, "Failed to create document: %s",
                db_strerror (dbret));
    }
    Py_RETURN_NONE;
}

/*
 * terane_Segment_get_event: retrieve an event
 *
 * callspec: Segment.get_event(txn, evid)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   evid (object): The event identifier
 * returns: The event
 * exceptions:
 *   KeyError: The event with the specified evid doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to retrieve the record
 */
PyObject *
terane_Segment_get_event (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *evid = NULL;
    DBT key, data;
    PyObject *event = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO", &txn, &evid))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    /* use the event identifier as the key */
    memset (&key, 0, sizeof (DBT));
    key.flags = DB_DBT_REALLOC;
    if (_terane_msgpack_dump (evid, (char **) &key.data, &key.size) < 0)
        return NULL;
    /* get the document */
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    dbret = self->events->get (self->events, txn? txn->txn : NULL, &key, &data, 0);
    switch (dbret) {
        case 0:
            /* create a python string from the data */
            _terane_msgpack_load ((char *) data.data, data.size, &event);
            break;
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            /* document doesn't exist, raise KeyError */
            PyErr_Format (PyExc_KeyError, "Event doesn't exist");
            break;
        default:
            /* some other db error, raise Error */
            PyErr_Format (terane_Exc_Error, "Failed to get event: %s",
                db_strerror (dbret));
            break;
    }
    /* free allocated memory */
    if (key.data)
        PyMem_Free (key.data);
    if (data.data)
        PyMem_Free (data.data);
    return event;
}

/*
 * terane_Segment_set_event: set the event contents
 *
 * callspec: Segment.set_event(txn, evid, event)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   evid (object): The event identifier
 *   event (object): Data to store in the event
 * returns: None
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_Segment_set_event (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *evid = NULL;
    PyObject *event = NULL;
    DBT key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!OO", &terane_TxnType, &txn, &evid, &event))
        return NULL;
    /* use the evid as the key */
    memset (&key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (evid, (char **) &key.data, &key.size) < 0)
        return NULL;
    /* set the event */
    memset (&data, 0, sizeof (DBT));
    if (_terane_msgpack_dump (event, (char **) &data.data, &data.size) < 0) {
        PyMem_Free (key.data);
        return NULL;
    }
    /* set the record */
    dbret = self->events->put (self->events, txn->txn, &key, &data, 0);
    PyMem_Free (key.data);
    PyMem_Free (data.data);
    /* db error, raise Exception */
    switch (dbret) {
        case 0:
            break;
        default:
            return PyErr_Format (terane_Exc_Error, "Failed to set document %s: %s",
                (char *) key.data, db_strerror (dbret));
    }
    Py_RETURN_NONE;
}

/*
 * terane_Segment_delete_event: Delete an event.
 *
 * callspec: Segment.delete_event(txn, evid)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   evid (object): The event identifier
 * returns: None
 * exceptions:
 *   KeyError: The event with the specified id doesn't exist
 *   terane.outputs.store.backend.Error: A db error occurred when trying to delete the record
 */
PyObject *
terane_Segment_delete_event (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *evid = NULL;
    DBT key;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!O", &terane_TxnType, &txn, &evid))
        return NULL;
    /* get the event id */
    memset (&key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (evid, (char **) &key.data, &key.size) < 0)
        return NULL;
    /* delete the record */
    dbret = self->events->del (self->events, txn->txn, &key, 0);
    PyMem_Free (key.data);
    switch (dbret) {
        case 0:
            break;
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            /* document doesn't exist, raise KeyError */
            return PyErr_Format (PyExc_KeyError, "Event %s doesn't exist",
                (char *) key.data);
        default:
            return PyErr_Format (terane_Exc_Error, "Failed to delete event %s: %s",
                (char *) key.data, db_strerror (dbret));
    }
    Py_RETURN_NONE;
}

/*
 * terane_Segment_contains_event: Determine whether an event exists.
 *
 * callspec: Segment.contains_event(txn, evid)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   evid (object): The event identifier
 * returns: True if the document exists, otherwise False.
 * exceptions:
 *   Exception: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_contains_event (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *evid = NULL;
    DBT key;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO", &txn, &evid))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    /* get the event id */
    memset (&key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (evid, (char **) &key.data, &key.size) < 0)
        return NULL;
    /* check for the record */
    dbret = self->events->exists (self->events, txn? txn->txn : NULL, &key, 0);
    PyMem_Free (key.data);
    switch (dbret) {
        case 0:
        case DB_NOTFOUND:
        case DB_KEYEMPTY:
            break;
        default:
            /* some other db error, raise Exception */
            return PyErr_Format (terane_Exc_Error, "Failed to find event %s: %s", 
                (char *) key.data, db_strerror (dbret));
    }
    if (dbret == 0)
        Py_RETURN_TRUE;
    Py_RETURN_FALSE;
}

/*
 * terane_Segment_estimate_events: Return an estimate of the number of
 *  events between the start and end event identifiers.
 *
 * callspec: Segment.estimate_events(txn, start, end)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   start (object): The starting event identifier
 *   end (object): The ending event identifier
 * returns: An estimate of the percentage of events between the start
 *  and end event identifiers, expressed as a float.
 * exceptions:
 *   terane.outputs.store.backend.Error: A db error occurred when trying to find the record
 */
PyObject *
terane_Segment_estimate_events (terane_Segment *self, PyObject *args)
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
    if (_terane_msgpack_dump (start, (char **) &start_key.data, &start_key.size) < 0)
        goto error;
    memset (&end_key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (end, (char **) &end_key.data, &end_key.size) < 0)
        goto error;

    /* estimate start key range */
    dbret = self->events->key_range (self->events, txn? txn->txn : NULL,
        &start_key, &start_range, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to estimate start key range: %s",
            db_strerror (dbret));
        goto error;
    }

    /* estimate end key range */
    dbret = self->events->key_range (self->events, txn? txn->txn : NULL,
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
 * _Segment_next_evid: build a (evid,None) tuple from the current cursor item
 */
static PyObject *
_Segment_next_evid (terane_Iter *iter, DBT *key, DBT *data)
{
    PyObject *evid, *tuple;

    /* get the event id */
    if (_terane_msgpack_load ((char *) key->data, key->size, &evid) < 0)
        return NULL;
    /* build the (evid,None) tuple */
    tuple = PyTuple_Pack (2, evid, Py_None);
    Py_XDECREF (evid);
    return tuple;
}

/*
 * terane_Segment_iter_events: Iterate through all event ids between
 * the specified start and end identifiers.
 *
 * callspec: Segment.iter_events(txn)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   start (object): The starting event identifier, inclusive
 *   end (object): The ending event identifier, inclusive
 * returns: a new Iterator object.  Each iteration returns a tuple consisting
 *  of (evid,None).
 * exceptions:
 *   Exception: A db error occurred when trying to get the record
 */
PyObject *
terane_Segment_iter_events (terane_Segment *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *start = NULL, *end = NULL;
    DBC *cursor = NULL;
    PyObject *iter = NULL;
    int dbret, reverse = 0;
    terane_Iter_ops ops = { .next = _Segment_next_evid };

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OOO", &txn, &start, &end))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* determine whether ordering is reversed */
    if (PyObject_Cmp (start, end, &reverse) < 0)
        return PyErr_Format (terane_Exc_Error, "comparison of start and end failed");
    if (reverse < 0)
        reverse = 0;

    /* create a new cursor */
    dbret = self->events->cursor (self->events, txn? txn->txn : NULL, &cursor, 0);
    /* if cursor allocation failed, return Error */
    if (dbret != 0)
        return PyErr_Format (terane_Exc_Error, "Failed to allocate document cursor: %s",
            db_strerror (dbret));

    /* allocate a new Iter object */
    iter = terane_Iter_new_within ((PyObject *) self, cursor,
        &ops, start, end, reverse);
    if (iter == NULL) {
        cursor->close (cursor);
        return NULL;
    }
    return iter;
}
