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

/* compare two terane_Field instances by name */
static int
_Segment_cmp_fields (const void *v1, const void *v2)
{
    const terane_Field *f1 = *(const terane_Field **) v1;
    const terane_Field *f2 = *(const terane_Field **) v2;
    return PyObject_Compare (f1->name, f2->name);
}

/* 
 * return the field specified by name, or NULL if it doesn't exist in the schema
 */
DB *
terane_Segment_get_field_DB (terane_Segment *self, terane_Txn *txn, PyObject *fieldname)
{
    terane_Field compar = {fieldname, NULL}, *compar_ptr = &compar;
    terane_Field **result = NULL;
    terane_Field *new = NULL, **fields = NULL;
    DB_TXN *field_txn = NULL;
    int dbret;

    /* search the field handle cache */
    result = (terane_Field **) bsearch (&compar_ptr, self->fields, self->nfields,
        sizeof(terane_Field *), _Segment_cmp_fields);
    /* we have the field handle cached, so return it */
    /* TODO: check whether the handle is stale (i.e. someone deleted the DB) */
    if (result && *result)
        return (*result)->field;

    /* if txn is specified, create a child transaction, otherwise create a new txn */
    dbret = self->index->env->env->txn_begin (self->index->env->env,
        txn? txn->txn : NULL, &field_txn, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create DB_TXN handle: %s",
            db_strerror (dbret));
        goto error;
    }

    /* allocate a new terane_Field record */
    new = PyMem_Malloc (sizeof (terane_Field));
    if (new == NULL) {
        PyErr_NoMemory ();
        goto error;
    }
    memset (new, 0, sizeof (terane_Field));

    /* increment the reference count for fieldname */
    Py_INCREF (fieldname);
    new->name = fieldname;

    /* allocate a new fields array, and copy the old fields content over.
     * we do this instead of realloc()ing the current fields array because
     * its easier to recover in a transactionally-safe way if this operation
     * fails.
     */
    fields = PyMem_Malloc (sizeof (terane_Field *) * (self->nfields + 1));
    if (fields == NULL) {
        PyErr_NoMemory ();
        goto error;
    }
    memcpy (fields, self->fields, sizeof (terane_Field *) * self->nfields);

    /* create the DB handle for the field */
    dbret = db_create (&new->field, self->index->env->env, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to create handle for %s: %s",
            PyString_AsString (fieldname), db_strerror (dbret));
        goto error;
    }

    /* open the field segment.  if the field doesn't exist its an error */
    dbret = new->field->open (new->field, field_txn, self->name,
        PyString_AsString (fieldname), DB_BTREE, DB_CREATE | DB_THREAD, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to open segment for %s: %s",
            PyString_AsString (fieldname), db_strerror (dbret));
        goto error;
    }

    /* commit database changes */
    dbret = field_txn->commit (field_txn, 0);
    if (dbret != 0) {
        PyErr_Format (terane_Exc_Error, "Failed to commit transaction: %s",
            db_strerror (dbret));
        goto error;
    }

    /* swap the old fields array with the new array */
    PyMem_Free (self->fields);
    self->fields = fields;

    /* sort the new fields array in alphabetical order */
    self->fields[self->nfields] = new;
    self->nfields++;
    qsort (self->fields, self->nfields, sizeof(terane_Field *), _Segment_cmp_fields);

    return new->field;

error:
    if (new != NULL) {
        if (new->field != NULL)
            new->field->close (new->field, 0);
        if (new->name != NULL)
            Py_DECREF (new->name);
        PyMem_Free (new);
    }
    if (fields != NULL)
        PyMem_Free (fields);
    if (field_txn != NULL)
        field_txn->abort (field_txn);
    return NULL;
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
    DBT key, data;
    DB *field;
    PyObject *metadata = NULL;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "OO!", &txn, &PyString_Type, &fieldname))
        return NULL;
    if ((PyObject *) txn == Py_None)
        txn = NULL;

    field = terane_Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* get the record */
    memset (&key, 0, sizeof (DBT));
    key.size = 1;
    key.data = "\0";
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    dbret = field->get (field, txn? txn->txn : NULL, &key, &data, 0);
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
 * callspec: Segment.set_word_meta(fieldname, metadata)
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
    DBT key, data;
    DB *field;
    int dbret;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O!O!s", &terane_TxnType, &txn,
        &PyString_Type, &fieldname, &metadata))
        return NULL;

    field = terane_Segment_get_field_DB (self, txn, fieldname);
    if (field == NULL)
        return NULL;

    /* set the record */
    memset (&key, 0, sizeof (DBT));
    memset (&data, 0, sizeof (DBT));
    key.size = 1;
    key.data = "\0";
    data.data = (char *) metadata;
    data.size = strlen (metadata) + 1;
    dbret = field->put (field, txn->txn, &key, &data, 0);
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
