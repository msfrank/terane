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

#include "storage.h"

/*
 * TOC_new_DID: allocate a new DID and return it as an unsigned 64-bit integer.
 * 
 * returns: An unsigned 64-bit integer identifier
 * exceptions:
 *  terane.db.storage.Error: failed to allocate new id
 */
terane_DID_num
TOC_new_DID (terane_TOC *toc)
{
    DBT key, data;
    DB_TXN *txn;
    terane_DID_num did_num;
    terane_DID_string did_string;
    int dbret;

    /* allocate a new transaction */
    dbret = toc->env->env->txn_begin (toc->env->env, NULL, &txn, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create document id: %s",
            db_strerror (dbret));
        return 0;
    }

    /* retrieve the current high document id */
    memset (&key, 0, sizeof (DBT));
    key.data = "last-id";
    key.size = 8;
    memset (&data, 0, sizeof (DBT));
    dbret = toc->metadata->get (toc->metadata, txn, &key, &data, DB_RMW);
    switch (dbret) {
        case 0:
            DID_string_to_num ((char *) data.data, &did_num);
            did_num++;
            break;
        case DB_NOTFOUND:
            did_num = 1;
            break;
        default:
            txn->abort (txn);
            PyErr_Format (terane_Exc_Error, "Failed to create document id: %s",
                db_strerror (dbret));
            return 0;
    }

    /* set the new high document id */
    memset (&key, 0, sizeof (DBT));
    key.data = "last-id";
    key.size = 8;
    memset (&data, 0, sizeof (DBT));
    DID_num_to_string (did_num, did_string);
    data.data = did_string;
    data.size = TERANE_DID_STRING_LEN;
    dbret = toc->metadata->put (toc->metadata, txn, &key, &data, 0);
    switch (dbret) {
        case 0:
            break;
        default:
            txn->abort (txn);
            PyErr_Format (terane_Exc_Error, "Failed to create document id: %s",
                db_strerror (dbret));
            return 0;
    }

    /* commit the transaction and return the docnum */
    txn->commit (txn, 0);
    return did_num;
}

/*
 * terane_TOC_get_metadata: retrieve a metadata item
 *
 * callspec: TOC.get_metadata(txn, id)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   id (string): The metadata id
 * returns: A string representing the metadata value 
 * exceptions:
 *   KeyError: The document with the specified id doesn't exist
 *   terane.db.storage.Error: A db error occurred when trying to retrieve the record
 */
PyObject *
terane_TOC_get_metadata (terane_TOC *self, PyObject *args)
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
    memset (&data, 0, sizeof (DBT));
    key.data = (char *) id;
    key.size = strlen(id) + 1;

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
    return metadata;
}

/*
 * terane_TOC_set_metadata: set metadata for the specified id
 *
 * callspec: TOC.set_metadata(txn, id, value)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   id (string): The metadata id
 *   value (string): Metadata to store
 * returns: None
 * exceptions:
 *   terane.db.storage.Error: A db error occurred when trying to set the record
 */
PyObject *
terane_TOC_set_metadata (terane_TOC *self, PyObject *args)
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

/*
 * terane_TOC_get_field: get the pickled fieldspec for the field.
 *
 * callspec: TOC.get_field(txn, fieldname)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 * returns: string representing the pickled FieldType.
 * exceptions:
 *   terane.db.storage.Error: A db error occurred when trying to get the field
 */
PyObject *
terane_TOC_get_field (terane_TOC *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    const char *fieldname = NULL;
    DBT key, data;
    PyObject *fieldspec = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "Os", &txn, &fieldname))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* use the document id as the record number */
    memset (&key, 0, sizeof (DBT));
    memset (&data, 0, sizeof (DBT));
    key.data = (char *) fieldname;
    key.size = strlen(fieldname) + 1;
    /* get the record */
    dbret = self->schema->get (self->schema, txn? txn->txn : NULL, &key, &data, 0);
    switch (dbret) {
        case 0:
            /* create a python string from the data */
            fieldspec = PyString_FromString ((char *) data.data);
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

    return fieldspec;
}

/*
 * terane_TOC_add_field: add the field to the Store.
 *
 * callspec: TOC.add_field(txn, fieldname, fieldspec)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 *   fieldname (string): The field name
 *   pickledfield (string): String representing the pickled FieldType
 * returns: None
 * exceptions:
 *   terane.db.storage.Error: A db error occurred when trying to add the field
 */
PyObject *
terane_TOC_add_field (terane_TOC *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    char *fieldname = NULL;
    char *pickledfield = NULL;
    DBT key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!ss", &terane_TxnType, &txn,
        &fieldname, &pickledfield))
        return NULL;

    /* add the fieldspec to the schema */
    memset (&key, 0, sizeof (DBT));
    memset (&data, 0, sizeof (DBT));
    key.data = fieldname;
    key.size = strlen (fieldname) + 1;
    data.data = pickledfield;
    data.size = strlen (pickledfield) + 1;
    dbret = self->schema->put (self->schema, txn->txn, &key, &data, DB_NOOVERWRITE);
    switch (dbret) {
        case 0:
            break;
        case DB_KEYEXIST:
            PyErr_Format (PyExc_KeyError, "Field %s already exists", fieldname);
            break;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to set fieldspec for %s: %s",
                fieldname, db_strerror (dbret));
            break;
    }

    Py_RETURN_NONE;
}

/*
 * terane_TOC_remove_field: remove a field from the index
 *
 * callspec: TOC.remove_field(txn, fieldname)
 * parameters:
 *   txn (Txn):
 *   fieldname (string): The field name
 * returns: None
 * exceptions:
 *   terane.db.storage.Error: A db error occurred when trying to remove the field
 */
PyObject *
terane_TOC_remove_field (terane_TOC *self, PyObject *args)
{
    return PyErr_Format (PyExc_NotImplementedError, "TOC.remove_field() not implemented");
}

/*
 * TOC_contains_field:
 */
int
TOC_contains_field (terane_TOC *toc, DB_TXN *txn, PyObject *fieldname)
{
    DBT key;
    int dbret;

    memset (&key, 0, sizeof (key));
    key.data = PyString_AsString (fieldname);
    key.size = PyString_Size (fieldname) + 1;
    dbret = toc->schema->exists (toc->schema, txn? txn : NULL, &key, 0);
    switch (dbret) {
        case 0:
            return 1;
        case DB_NOTFOUND:
            return 0;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to lookup field %s in schema: %s",
                PyString_AsString (fieldname), db_strerror (dbret));
            break;
    }
    return -1;
}

/*
 * terane_TOC_contains_field: return True if field exists in the schema
 *
 * callspec: TOC.contains_field(txn, fieldname)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   fieldname (string): The field name
 * returns: True if the field exists, otherwise False.
 * exceptions:
 *   terane.db.storage.Error: A db error occurred when trying to get the fields
 */
PyObject *
terane_TOC_contains_field (terane_TOC *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *fieldname = NULL;
    int ret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!", &txn, &PyString_Type, &fieldname))
        return NULL;
    if ((PyObject *)txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");
    ret = TOC_contains_field (self, txn? txn->txn : NULL, fieldname);
    if (ret > 0)
        Py_RETURN_TRUE;
    if (ret == 0)
        Py_RETURN_FALSE;
    return NULL;
}

/*
 * terane_TOC_list_fields: return a list of the fields in the schema
 *
 * callspec: TOC.list_fields(txn)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 * returns: a list of (fieldname,pickledfield) tuples.
 * exceptions:
 *   terane.db.storage.Error: A db error occurred when trying to get the fields
 */
PyObject *
terane_TOC_list_fields (terane_TOC *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    DBC *cursor = NULL;
    PyObject *fields = NULL, *tuple = NULL, *fieldname = NULL, *pickledfield = NULL;
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
        memset (&data, 0, sizeof (DBT));
        dbret = cursor->get (cursor, &key, &data, DB_NEXT);
        if (dbret == DB_NOTFOUND)
            break;
        switch (dbret) {
            case 0:
                fieldname = PyString_FromStringAndSize (key.data, key.size - 1);
                if (fieldname == NULL)
                    goto error;
                pickledfield = PyString_FromStringAndSize (data.data, data.size - 1);
                if (pickledfield == NULL)
                    goto error;
                tuple = PyTuple_Pack (2, fieldname, pickledfield);
                if (tuple == NULL)
                    goto error;
                if (PyList_Append (fields, tuple) < 0)
                    goto error;
                Py_DECREF (fieldname);
                fieldname = NULL;
                Py_DECREF (pickledfield);
                pickledfield = NULL;
                Py_DECREF (tuple);
                tuple = NULL;
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
    if (pickledfield != NULL)
        Py_DECREF (pickledfield);
    if (tuple != NULL)
        Py_DECREF (tuple);
    if (fields != NULL)
        Py_DECREF (fields);
    return NULL;
}

/*
 * terane_TOC_count_fields: Return the number of fields in the schema.
 *
 * callspec: TOC.count_fields(txn, slow=False)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   slow (boolean): If True, then perform a slower, more accurate count
 * returns: The number of fields in the schema. 
 * exceptions:
 *   terane.db.storage.Error: A db error occurred when trying to get the record
 */
PyObject *
terane_TOC_count_fields (terane_TOC *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *slow = NULL;
    DB_BTREE_STAT *stats = NULL;
    PyObject *count = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O|O", &txn, &slow))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* retrieve count of items in the schema */
    if (slow && PyObject_IsTrue (slow) == 1)
        dbret = self->schema->stat (self->schema, txn? txn->txn : NULL, &stats, 0);
    else
        dbret = self->schema->stat (self->schema, txn? txn->txn : NULL, &stats, DB_FAST_STAT);
    switch (dbret) {
        case 0:
            count = PyLong_FromUnsignedLong ((unsigned long) stats->bt_nkeys);
            break;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to get field count: %s",
                db_strerror (dbret));
            break;
    }
    if (stats)
        PyMem_Free (stats);
    return count;
 }

/*
 * terane_TOC_new_segment: allocate a new Segment id.
 *
 * callspec: TOC.new_segment(txn)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in
 * returns: A long representing the segment id.
 * exceptions:
 *   terane.db.storage.Error: A db error occurred when trying to allocate the segment.
 */
PyObject *
terane_TOC_new_segment (terane_TOC *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    db_recno_t segment_id = 0;
    DBT key, data;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!", &terane_TxnType, &txn))
        return NULL;

    /* add the fieldspec to the schema */
    memset (&key, 0, sizeof (DBT));
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

    segment_id = *((db_recno_t *) key.data);
    return PyLong_FromUnsignedLong ((unsigned long) segment_id);
}

/*
 * TOC_contains_segment: return true if segment exists in the TOC
 */
int
TOC_contains_segment (terane_TOC *toc, terane_Txn *txn, db_recno_t segment_id)
{
    DBT key;
    int dbret;

    memset (&key, 0, sizeof (key));
    key.data = &segment_id;
    key.size = sizeof (segment_id);
    dbret = toc->schema->exists (toc->segments, txn? txn->txn : NULL, &key, 0);
    switch (dbret) {
        case 0:
            return 1;
        case DB_NOTFOUND:
            return 0;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to lookup segment %lu in segments: %s",
                (unsigned long int) segment_id, db_strerror (dbret));
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
    db_recno_t segmentid = 0;

    segmentid = *((db_recno_t *) key->data);
    return PyLong_FromUnsignedLong ((unsigned long) segmentid);
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
 *   terane.db.storage.Error: A db error occurred when trying to create the iterator
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
    iter = Iter_new ((PyObject *) self, cursor, &ops);
    if (iter == NULL)
        cursor->close (cursor);
    return iter;
}

/*
 * terane_TOC_count_segments: return the number of segments in the TOC.
 *
 * callspec: TOC.count_segments(txn, slow=False)
 * parameters:
 *   txn (Txn): A Txn object to wrap the operation in, or None
 *   slow (boolean): If True, then perform a slower, more accurate count
 * returns: The number of segments in the TOC
 * exceptions:
 *   terane.db.storage.Error: A db error occurred when trying count the segments
 */
PyObject *
terane_TOC_count_segments (terane_TOC *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    PyObject *slow = NULL;
    DB_QUEUE_STAT *stats = NULL;
    PyObject *count = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O|O", &txn, &slow))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;
    if (txn && txn->ob_type != &terane_TxnType)
        return PyErr_Format (PyExc_TypeError, "txn must be a Txn or None");

    /* retrieve count of items in the schema */
    dbret = self->segments->stat (self->segments, txn? txn->txn : NULL,
        &stats, slow && PyObject_IsTrue (slow)? 0 : DB_FAST_STAT);
    switch (dbret) {
        case 0:
            count = PyLong_FromUnsignedLong ((unsigned long int) stats->qs_nkeys);
            break;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to get segment count: %s",
                db_strerror (dbret));
            break;
    }
    if (stats)
        PyMem_Free (stats);
    return count;
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
 *   terane.db.storage.Error: A db error occurred when trying count the segments
 */PyObject *
terane_TOC_delete_segment (terane_TOC *self, PyObject *args)
{
    terane_Txn *txn = NULL;
    unsigned long segment_id= 0;
    DBT key;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!k", &terane_TxnType, &txn, &segment_id))
        return NULL;

    /* delete segment id from the TOC */
    memset (&key, 0, sizeof (DBT));
    key.data = &segment_id;
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
    Py_RETURN_NONE;
}

/*
 * _TOC_close: close the underlying DB handles.
 */
static void
_TOC_close (terane_TOC *toc)
{
    int dbret;

    /* close the metadata db */
    if (toc->metadata != NULL) {
        dbret = toc->metadata->close (toc->metadata, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close _metadata: %s",
                db_strerror (dbret));
    }
    toc->metadata = NULL;

    /* close the schema db */
    if (toc->schema != NULL) {
        dbret = toc->schema->close (toc->schema, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close _schema: %s",
                db_strerror (dbret));
    }
    toc->schema = NULL;

    /* close the segments db */
    if (toc->segments != NULL) {
        dbret = toc->segments->close (toc->segments, 0);
        if (dbret != 0)
            PyErr_Format (terane_Exc_Error, "Failed to close _segments: %s",
                db_strerror (dbret));
    }
    toc->segments = NULL;
}

/*
 * terane_TOC_close: close the underlying DB handles.
 *
 * callspec: TOC.close()
 * parameters: None
 * returns: None
 * exceptions:
 *  terane.db.storage.Error: failed to close a db in the TOC
 */
PyObject *
terane_TOC_close (terane_TOC *self)
{
    _TOC_close (self);
    Py_RETURN_NONE;
}

/*
 * terane_TOC_dealloc: free resources for the TOC object.
 */
static void
_TOC_dealloc (terane_TOC *self)
{
    _TOC_close (self);
    if (self->env != NULL)
        Py_DECREF (self->env);
    self->env = NULL;
    if (self->name != NULL)
        Py_DECREF (self->name);
    self->name = NULL;
    self->ob_type->tp_free ((PyObject *) self);
}

/*
 * terane_TOC_new: allocate a new TOC object.
 *
 * callspec: TOC(env, name)
 * parameters:
 *  env (Env): A Env object to use as the environment
 *  name (string): The name of the Index
 * returns: A new TOC object
 * exceptions:
 *  terane.db.storage.Error: failed to create/open the TOC
 */
PyObject *
terane_TOC_new (PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    terane_TOC *self;
    char *kwlist[] = {"env", "name", NULL};
    char *tocname = NULL;
    DB_TXN *txn = NULL;
    int dbret;

    /* allocate the TOC object */
    self = (terane_TOC *) type->tp_alloc (type, 0);
    if (self == NULL) {
        PyErr_SetString (terane_Exc_Error, "Failed to allocate TOC");
        return NULL;
    }
    self->name = NULL;
    self->metadata = NULL;
    self->schema = NULL;
    self->segments = NULL;

    /* parse constructor parameters */
    if (!PyArg_ParseTupleAndKeywords (args, kwds, "O!O!", kwlist,
        &terane_EnvType, &self->env, &PyString_Type, &self->name))
        goto error;
    Py_INCREF (self->env);
    Py_INCREF (self->name);

    /* allocate a buffer with the full index toc name */
    tocname = PyMem_Malloc (PyString_Size (self->name) + 5);
    if (tocname == NULL) {
        PyErr_NoMemory ();
        goto error;
    }
    sprintf (tocname, "%s.toc", PyString_AsString (self->name));

    /* wrap db creation in a transaction */
    dbret = self->env->env->txn_begin (self->env->env, NULL, &txn, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create DB_TXN handle: %s",
            db_strerror (dbret));
        goto error;
    }

    /* create the DB handle for the metadata store */
    dbret = db_create (&self->metadata, self->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for _metadata: %s",
            db_strerror (dbret));
        goto error;
    }
    /* open the metadata store */
    dbret = self->metadata->open (self->metadata, txn, tocname, "_metadata",
        DB_BTREE, DB_CREATE, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open _metadata: %s",
            db_strerror (dbret));
        goto error;
    }

    /* create the DB handle for the schema store */
    dbret = db_create (&self->schema, self->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for _schema: %s",
            db_strerror (dbret));
        goto error;
    }
    /* open the schema store */
    dbret = self->schema->open (self->schema, txn, tocname, "_schema",
        DB_BTREE, DB_CREATE, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open _schema: %s",
            db_strerror (dbret));
        goto error;
    }
 
    /* create the DB handle for the segments store */
    dbret = db_create (&self->segments, self->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for _segments: %s",
            db_strerror (dbret));
        goto error;
    }
    /* open the segments store */
    dbret = self->segments->open (self->segments, txn, tocname, "_segments",
        DB_RECNO, DB_CREATE, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open _segments: %s",
            db_strerror (dbret));
        goto error;
    }
   
    /* commit new databases */
    dbret = txn->commit (txn, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to commit transaction: %s",
            db_strerror (dbret));
        goto error;
    }

    PyMem_Free (tocname);

    /* return the initialized TOC object on success */
    return (PyObject *) self;

/* if there is an error, then free any locally allocated memory and references */
error:
    if (txn != NULL)
        txn->abort (txn);
    if (tocname != NULL)
        PyMem_Free (tocname);
    if (self)
        _TOC_dealloc ((terane_TOC *) self);
    return NULL;
}

/* TOC methods declaration */
PyMethodDef _TOC_methods[] =
{
    { "get_metadata", (PyCFunction) terane_TOC_get_metadata, METH_VARARGS,
        "Get a TOC metadata value." },
    { "set_metadata", (PyCFunction) terane_TOC_set_metadata, METH_VARARGS,
        "Set a TOC metadata value." },
    { "get_field", (PyCFunction) terane_TOC_get_field, METH_VARARGS,
        "Get a field in the TOC." },
    { "add_field", (PyCFunction) terane_TOC_add_field, METH_VARARGS,
        "Add a field to the TOC." },
    { "remove_field", (PyCFunction) terane_TOC_remove_field, METH_VARARGS,
        "Remove a field from the TOC." },
    { "contains_field", (PyCFunction) terane_TOC_contains_field, METH_VARARGS,
        "Return True if the field exists in the TOC." },
    { "list_fields", (PyCFunction) terane_TOC_list_fields, METH_VARARGS,
        "Return a list of all fields in the TOC." },
    { "count_fields", (PyCFunction) terane_TOC_count_fields, METH_VARARGS,
        "Return the count of fields in the TOC." },
    { "new_segment", (PyCFunction) terane_TOC_new_segment, METH_VARARGS,
        "Allocate a new segment ID." },
    { "iter_segments", (PyCFunction) terane_TOC_iter_segments, METH_VARARGS,
        "Iterate all segment IDs in the TOC." },
    { "count_segments", (PyCFunction) terane_TOC_count_segments, METH_VARARGS,
        "Return the count of segments in the TOC." },
    { "delete_segment", (PyCFunction) terane_TOC_delete_segment, METH_VARARGS,
        "Delete the segment from the TOC." },
    { "close", (PyCFunction) terane_TOC_close, METH_NOARGS,
        "Close the TOC." },
    { NULL, NULL, 0, NULL }
};

/* TOC type declaration */
PyTypeObject terane_TOCType = {
    PyObject_HEAD_INIT(NULL)
    0,
    "storage.TOC",
    sizeof (terane_TOC),
    0,                         /*tp_itemsize*/
    (destructor) _TOC_dealloc,
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,        /*tp_flags*/
    "DB TOC",                  /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    0,                         /* tp_iter */
    0,                         /* tp_iternext */
    _TOC_methods,
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    terane_TOC_new
};
