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
 * _free_iterkey:
 */
static void
_free_iterkey (terane_iterkey *iter_key)
{
    int i;

    if (iter_key == NULL)
        return;
    if (iter_key->values) {
        for (i = 0; i < iter_key->size; i++) {
            if (iter_key->values[i])
                _terane_msgpack_free_value (iter_key->values[i]);
        }
        PyMem_Free (iter_key->values);
    }
    PyMem_Free (iter_key);
}

/*
 * _make_iterkey:
 */
static terane_iterkey *
_make_iterkey (PyObject *list)
{
    terane_iterkey *iter_key = NULL;
    Py_ssize_t size;
    int i;

    assert (PyList_CheckExact (list));

    iter_key = PyMem_Malloc (sizeof (terane_iterkey));
    if (iter_key == NULL)
        return NULL;
    size = PyList_GET_SIZE (list);
    iter_key->values = PyMem_New (terane_value *, size);
    if (iter_key->values == NULL) {
        _free_iterkey (iter_key);
        return NULL;
    }
    memset (iter_key->values, 0, sizeof (terane_value *) * size);
    iter_key->size = size;
    for (i = 0; i < iter_key->size; i++ ) {
        PyObject *item = PyList_GET_ITEM (list, i);
        iter_key->values[i] = _terane_msgpack_make_value (item);
        if (iter_key->values[i] < 0) {
            _free_iterkey (iter_key);
            return NULL;
        }
    }
    return iter_key;
}

/*
 * terane_Iter_new: allocate a new Iter object using the supplied DB cursor.
 * 
 * returns: A new Iter object
 */
PyObject *
terane_Iter_new (PyObject *             parent,
                 DBC *                  cursor,
                 terane_Iter_ops *      ops,
                 int                    reverse)
{
    terane_Iter *iter;

    assert (parent != NULL);
    assert (cursor != NULL);
    assert (ops != NULL);

    iter = PyObject_New (terane_Iter, &terane_IterType);
    if (iter == NULL)
        return NULL;
    Py_INCREF (parent);
    /* PyObject_New doesn't set fields to 0, so make sure we initialize all fields */
    iter->parent = parent;
    iter->cursor = cursor;
    iter->initialized = 0;
    iter->itype = TERANE_ITER_ALL;
    iter->start = NULL;
    iter->end = NULL;
    memset (&iter->range, 0, sizeof (DBT));
    iter->next = ops->next;
    iter->skip = ops->skip;
    iter->reverse = reverse;
    return (PyObject *) iter;
}


/*
 * terane_Iter_new_range: allocate a new Iter object using the supplied DB
 *  cursor.
 * 
 * returns: A new Iter object
 */
PyObject *
terane_Iter_new_range (PyObject *           parent,
                       DBC *                cursor,
                       terane_Iter_ops *    ops,
                       PyObject *           key,
                       int                  reverse)
{
    terane_Iter *iter;

    assert (PyTuple_CheckExact (key));

    iter = (terane_Iter *) terane_Iter_new (parent, cursor, ops, reverse);
    if (iter == NULL)
        return NULL;
    iter->itype = TERANE_ITER_RANGE;
    iter->start = _make_iterkey (key);
    if (iter->start == NULL) {
        Py_DECREF (iter);
        return NULL;
    }
    if (_terane_msgpack_dump (key, (char **) &iter->range.data, &iter->range.size) < 0) {
        Py_DECREF (iter);
        return NULL;
    }
    return (PyObject *) iter;
}

/*
 * terane_Iter_new_from: allocate a new Iter object using the supplied DB cursor.
 * 
 * returns: A new Iter object
 */
PyObject *
terane_Iter_new_from (PyObject *            parent,
                      DBC *                 cursor,
                      terane_Iter_ops *     ops,
                      PyObject *            key,
                      int                   reverse)
{
    terane_Iter *iter;

    assert (PyTuple_CheckExact (key));

    iter = (terane_Iter *) terane_Iter_new (parent, cursor, ops, reverse);
    if (iter == NULL)
        return NULL;
    iter->itype = TERANE_ITER_FROM;
    iter->start = _make_iterkey (key);
    if (iter->start == NULL) {
        Py_DECREF (iter);
        return NULL;
    }
    if (_terane_msgpack_dump (key, (char **) &iter->range.data, &iter->range.size) < 0) {
        Py_DECREF (iter);
        return NULL;
    }
    return (PyObject *) iter;
}

/*
 * terane_Iter_new_within: allocate a new Iter object using the supplied DB cursor.
 * 
 * returns: A new Iter object
 */
PyObject *
terane_Iter_new_within (PyObject *          parent,
                        DBC *               cursor,
                        terane_Iter_ops *   ops,
                        PyObject *          start,
                        PyObject *          end,
                        int                 reverse)
{
    terane_Iter *iter;

    assert (PyTuple_CheckExact (start));
    assert (PyTuple_CheckExact (end));

    iter = (terane_Iter *) terane_Iter_new (parent, cursor, ops, reverse);
    if (iter == NULL)
        return NULL;
    iter->itype = TERANE_ITER_WITHIN;
    iter->start = _make_iterkey (start);
    if (iter->start == NULL) {
        Py_DECREF (iter);
        return NULL;
    }
    iter->end = _make_iterkey (end);
    if (iter->end == NULL) {
        Py_DECREF (iter);
        return NULL;
    }
    if (_terane_msgpack_dump (start, (char **) &iter->range.data, &iter->range.size) < 0) {
        Py_DECREF (iter);
        return NULL;
    }
    return (PyObject *) iter;
}

/*
 * _Iter_iter: return the iterator.
 */
static PyObject *
_Iter_iter (terane_Iter *self)
{
    Py_XINCREF (self);
    return (PyObject *) self;
}

/*
 * _Iter_cmp: compare lhs and rhs, and set *result to be less than, equal
 *  to, or greater than zero if the lhr is less than, equal to, or greater
 *  than the rhs.
 */
static int
_Iter_cmp (terane_iterkey *lhs, DBT *rhs, int reverse, int *result)
{
    char *pos = NULL;
    terane_value value;
    int i, ret = 0;

    /* compare each item */
    for (i = 0; i < lhs->size; i++) {
        ret = _terane_msgpack_load_value (rhs->data, rhs->size, &pos, &value);
        if (ret < 0)
            PyErr_Format (PyExc_ValueError, "cmp failed: error loading value for rhs");
        if (ret > 0) {
            ret = _terane_msgpack_cmp_values (lhs->values[i], &value);
            if (ret < 0)
                PyErr_Format (PyExc_ValueError, "cmp failed: error comparing values");
        }
        else if (ret == 0)
            *result = 1;
        if (ret < 0 || *result != 0)
            break;
    }
    /* if the comparison is still equal */
    if (ret == 0 && *result == 0) {
        /* if there is more data to serialize */
        if (pos - (char *)rhs->data > rhs->size)
            *result = -1;
    }
    return ret;
}

/*
 * _Iter_get: retreive the iterator item from the cursor.
 */
static PyObject *
_Iter_get (terane_Iter *iter, int itype, int flags, DBT *range_key)
{
    DBT key, data;
    int dbret, result = 0;
    PyObject *item = NULL;

    /* set the range key, if applicable */
    memset (&key, 0, sizeof (DBT));
    if (range_key) {
        key.data = range_key->data;
        key.size = range_key->size;
    }
    key.flags = DB_DBT_MALLOC;

    /* get the next cursor item */
    memset (&data, 0, sizeof (DBT));
    data.flags = DB_DBT_MALLOC;
    dbret = iter->cursor->get (iter->cursor, &key, &data, flags);    
    switch (dbret) {
        /* success */
        case 0:
            /* we have now completed one successful iteration */ 
            iter->initialized = 1;
            break;
        case DB_NOTFOUND:
            /* if no item is found, then return NULL */
            return NULL;
        /* for any other error, set exception and return NULL */
        case DB_LOCK_DEADLOCK:
            PyErr_Format (terane_Exc_Deadlock, "Failed to get next item: %s",
                db_strerror (dbret));
            return NULL;
        case DB_LOCK_NOTGRANTED:
            PyErr_Format (terane_Exc_LockTimeout, "Failed to get next item: %s",
                db_strerror (dbret));
            return NULL;
        default:
            PyErr_Format (terane_Exc_Error, "Failed to get next item: %s",
                db_strerror (dbret));
            return NULL;
    }

    /* perform post-retrieval checks */
    switch (itype) {
        case TERANE_ITER_RANGE:
            /* check that the key prefix matches */
            if (_Iter_cmp (iter->start, &key, iter->reverse, &result) < 0)
                break;
            if (result > 0)
                item = iter->next (iter, &key, &data);
            break;
        case TERANE_ITER_WITHIN:
            /* check that the key is between the start and end keys */
            if (_Iter_cmp (iter->start, &key, iter->reverse, &result) < 0)
                break;
            if (result <= 0)
                break;
            if (_Iter_cmp (iter->end, &key, iter->reverse, &result) < 0)
                break;
            if (result <= 0)
                break;
            item = iter->next (iter, &key, &data);
            break;
        default:
            item = iter->next (iter, &key, &data);
            break;
    }

    /* free allocated memory */
    if (key.data)
        PyMem_Free (key.data);
    if (data.data)
        PyMem_Free (data.data);

    return item;
}

/*
 * _Iter_next: return the next iterator item.
 */
static PyObject *
_Iter_next (terane_Iter *self)
{
    PyObject *item = NULL;
    uint32_t flags = 0;

    if (self->cursor == NULL)
        return PyErr_Format (terane_Exc_Error, "iterator is closed");
    if (self->next == NULL)
        return PyErr_Format (terane_Exc_Error, "No next callback for iterator");

    /* initialize the cursor position, if necessary */
    switch (self->itype)
    {
        case TERANE_ITER_ALL:
            if (!self->reverse)
                flags = !self->initialized ?  DB_FIRST : DB_NEXT;
            else
                flags = !self->initialized ?  DB_LAST : DB_PREV;
            break;
        case TERANE_ITER_RANGE:
        case TERANE_ITER_FROM:
        case TERANE_ITER_WITHIN:
            if (!self->initialized) {
                /* find the key to start iterating at */
                flags = DB_SET_RANGE;
                if (self->reverse) {
                    /* peek at the first key, it may be past our start */
                    item = _Iter_get (self, self->itype, flags, &self->range);
                    /* if the first record is valid, then return it */
                    if (item != NULL)
                        return item;
                    /* otherwise we consume it and skip to the next record */
                    flags = DB_PREV;
                }
            }
            else
                flags = self->reverse? DB_PREV : DB_NEXT;
            break;
        default:
            return PyErr_Format (terane_Exc_Error, "No iterator type %i", self->itype);
    }
    item = _Iter_get (self, self->itype, flags, NULL);
    if (item == NULL)
        terane_Iter_close (self);
    return item;
}

/*
 * terane_Iter_skip: Move the iterator to the specified item.
 *
 * callspec: Iter.skip(item)
 * parameters:
 *   item (object): A python object that describes the item to skip to
 * returns: The iterator value at the skipped-to position
 * exceptions:
 *  IndexError: Target item is out of range
 *  terane.outputs.store.backend.Error: failed to move the DBC cursor
 */
PyObject *
terane_Iter_skip (terane_Iter *self, PyObject *args)
{
    DBT skip_key;
    PyObject *skip_obj = NULL;
    PyObject *item = NULL;
    int itype;

    if (self->cursor == NULL)
        return PyErr_Format (terane_Exc_Error, "iterator is closed");
    if (self->skip == NULL)
        return PyErr_Format (terane_Exc_Error, "No skip callback for iterator");

    /* generate the key to skip to, or return NULL */
    skip_obj = self->skip (self, args);
     /* The skip function should set exception if it returns NULL */
    if (skip_obj == NULL)
        return NULL;
    /* if the skip_obj is not a tuple, raise ValueError */
    if (!PyTuple_CheckExact (skip_obj)) {
        Py_DECREF (skip_obj);
        return PyErr_Format (PyExc_ValueError, "skip target must be a tuple");
    }
    /* dump the skip key object */
    memset (&skip_key, 0, sizeof (DBT));
    if (_terane_msgpack_dump (skip_obj, (char **) &skip_key.data, &skip_key.size) < 0) {
        Py_DECREF (skip_obj);
        return NULL;
    }
    Py_DECREF (skip_obj);
    /* set the itype in order to correctly perform post-retrieval check */
    if (self->itype == TERANE_ITER_WITHIN)
        itype = TERANE_ITER_WITHIN;
    else
        itype = TERANE_ITER_RANGE;
    /* retrieve the item associated with the key */
    item = _Iter_get (self, itype, DB_SET, &skip_key);
    PyMem_Free (skip_key.data);
    /* raise IndexError if the item was not found */
    if (item == NULL && !PyErr_Occurred())
        return PyErr_Format (PyExc_IndexError, "Target ID does not exist");
    /* otherwise return the item */
    return item;
}

/*
 * terane_Iter_close: close the underlying DB Cursor.
 *
 * callspec: Iter.close()
 * parameters: None
 * returns: None
 * exceptions:
 *  terane.outputs.store.backend.Deadlock: the DBC cursor was chosen by deadlock detector
 *  terane.outputs.store.backend.LockTimeout:
 *  terane.outputs.store.backend.Error: failed to close the DBC cursor
 */
PyObject *
terane_Iter_close (terane_Iter *self)
{
    if (self->cursor != NULL) {
        int dbret = self->cursor->close (self->cursor);
        switch (dbret) {
            case 0:
                break;
            case DB_LOCK_DEADLOCK:
                PyErr_Format (terane_Exc_Deadlock, "Failed to close Iter: %s", db_strerror (dbret));
                break;
            case DB_LOCK_NOTGRANTED:
                PyErr_Format (terane_Exc_LockTimeout, "Failed to close Iter: %s", db_strerror (dbret));
                break;
            default:
                PyErr_Format (terane_Exc_Error, "Failed to close Iter: %s", db_strerror (dbret));
                break;
        }
        self->cursor = NULL;
    }
    if (self->parent)
        Py_DECREF (self->parent);
    self->parent = NULL;
    if (self->start)
        _free_iterkey (self->start);
    self->start = NULL;
    if (self->end)
        _free_iterkey (self->end);
    self->end = NULL;
    if (self->range.data)
        PyMem_Free (self->range.data);
    self->range.data = NULL;
    Py_RETURN_NONE;
}

/*
 * _Iter_dealloc: free resources for the Iter object.
 */
static void
_Iter_dealloc (terane_Iter *self)
{
    terane_Iter_close (self);
    self->ob_type->tp_free ((PyObject *) self);
}

/* Iter methods declaration */
PyMethodDef _Iter_methods[] =
{
    { "skip", (PyCFunction) terane_Iter_skip, METH_VARARGS,
        "Move the iterator to the specified item." },
    { "close", (PyCFunction) terane_Iter_close, METH_NOARGS,
        "Free resources allocated by the iterator." },
    { NULL, NULL, 0, NULL }
};

/* Iter type declaration */
PyTypeObject terane_IterType = {
    PyObject_HEAD_INIT(NULL)
    0,
    "backend.Iter",
    sizeof (terane_Iter),
    0,                         /*tp_itemsize*/
    (destructor) _Iter_dealloc,
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
    "Generic DB Iterator",     /* tp_doc */
    0,                         /* tp_traverse */
    0,                         /* tp_clear */
    0,                         /* tp_richcompare */
    0,                         /* tp_weaklistoffset */
    (getiterfunc) _Iter_iter,
    (iternextfunc) _Iter_next,
    _Iter_methods,
    0,                         /* tp_members */
    0,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    0,                         /* tp_init */
    0,                         /* tp_alloc */
    0,                         /* tp_new */
};
