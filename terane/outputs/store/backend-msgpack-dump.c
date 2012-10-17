/*
 * Copyright 2012 Michael Frank <msfrank@syntaxjockey.com>
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
 * _msgpack_dump_object:
 */
static int
_msgpack_dump_object (PyObject *obj, msgpack_packer *packer)
{
    int32_t i32;
    uint64_t u64;
    int64_t i64;
    double d;
    char *s;
    PyObject *utf8, *seq, **items, *key, *value;
    Py_ssize_t size, i = 0;

    if (obj == NULL)
        return 0;
    /* None */
    if (obj == Py_None) {
        msgpack_pack_nil (packer);
    }
    /* True */
    else if (obj == Py_True) {
        msgpack_pack_true (packer);
    }
    /* False */
    else if (obj == Py_False) {
        msgpack_pack_false (packer);
    }
    /* int */
    else if (PyInt_CheckExact (obj)) {
        i32 = PyInt_AsLong (obj);
        msgpack_pack_int32 (packer, i32);
    }
    /* long */
    else if (PyLong_CheckExact (obj)) {
        i64 = PyLong_AsLongLong (obj);
        if (!PyErr_Occurred())
            msgpack_pack_int64 (packer, i64);
        else {
            if (!PyErr_ExceptionMatches (PyExc_OverflowError))
                return -1;
            PyErr_Clear ();
            u64 = PyLong_AsUnsignedLongLong (obj);
            if (PyErr_Occurred())
                return -1;
            msgpack_pack_uint64 (packer, u64);
        }
    }
    /* float */
    else if (PyFloat_CheckExact (obj)) {
        d = PyFloat_AsDouble (obj);
        msgpack_pack_double (packer, d);
    }
    /* unicode */
    else if (PyUnicode_CheckExact (obj)) {
        utf8 = PyUnicode_AsUTF8String (obj);
        PyString_AsStringAndSize (utf8, &s, &size);
        msgpack_pack_raw (packer, size);
        msgpack_pack_raw_body (packer, s, size);
        Py_DECREF (utf8);
    }
    /* list */
    else if (PyList_CheckExact (obj)) {
        seq = PySequence_Fast (obj, NULL);
        size = PySequence_Fast_GET_SIZE (seq);
        items = PySequence_Fast_ITEMS (seq);
        msgpack_pack_array (packer, size);
        for (i = 0; i < size; i++) {
            if (_msgpack_dump_object (items[i], packer) < 0)
                return -1;
        }
    }
    /* dict */
    else if (PyDict_CheckExact (obj)) {
        size = PyDict_Size (obj);
        msgpack_pack_map (packer, size);
        while (PyDict_Next (obj, &i, &key, &value)) {
            if (_msgpack_dump_object (key, packer) < 0)
                return -1;
            if (_msgpack_dump_object (value, packer) < 0)
                return -1;
        }
    }
    /* unknown type */
    else {
        PyErr_Format (PyExc_ValueError, "can't serialize type %s", obj->ob_type->tp_name);
        return -1;
    }
    return 0;
}

struct _buffer {
    char *data;
    uint32_t size;
};

static int
_buffer_write (struct _buffer *buffer, const char *buf, unsigned int len)
{
    char *newdata;
    uint32_t newsize;

    newsize = buffer->size + len;
    newdata = PyMem_Realloc (buffer->data, (size_t) newsize);
    if (newdata == NULL)
        return -1;
    memcpy (newdata + buffer->size, buf, (size_t) len);
    buffer->data = newdata;
    buffer->size = newsize;
    return 0;
}

/*
 * _terane_msgpack_dump: serialize a python object into a buffer.
 */
int
_terane_msgpack_dump (PyObject *obj, char **buf, uint32_t *len)
{
    struct _buffer buffer;
    msgpack_packer packer;
    PyObject *seq, **items;
    Py_ssize_t nitems, i;

    memset (&buffer, 0, sizeof (buffer));
    msgpack_packer_init (&packer, &buffer, (msgpack_packer_write) _buffer_write);

    if (PyList_CheckExact (obj)) {
        seq = PySequence_Fast (obj, NULL);
        nitems = PySequence_Fast_GET_SIZE (seq);
        items = PySequence_Fast_ITEMS (seq);
        for (i = 0; i < nitems; i++) {
            if (_msgpack_dump_object (items[i], &packer) < 0)
                goto error;
        }
    }
    else {
        if (_msgpack_dump_object (obj, &packer) < 0)
            goto error;
    }
    *buf = buffer.data;
    *len = buffer.size;
    return 0;

error:
    if (buffer.data)
        PyMem_Free (buffer.data);
    return -1;
}

/*
 * terane_msgpack_dump: serialize a python object into a buffer.
 *
 * callspec: msgpack_dump(obj)
 *  obj (object): A python object to serialize. 
 * returns:
 * exceptions:
 */
PyObject *
terane_msgpack_dump (PyObject *self, PyObject *args)
{
    PyObject *obj = NULL;
    PyObject *str = NULL;
    char *buf = NULL;
    uint32_t len = 0;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "O", &obj))
        return NULL;
    /* dump the object into buf */
    if (_terane_msgpack_dump (obj, &buf, &len) < 0)
        return NULL;
    /* build a string from the buf */
    str = PyString_FromStringAndSize (buf, (Py_ssize_t) len);
    if (buf)
        PyMem_Free (buf);
    return str;
}
