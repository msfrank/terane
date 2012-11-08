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

#define HTONS(s)        htons(s)
#define HTONL(l)        htonl(l)
#define HTONQ(q)        htobe64(q)

/*
 * _terane_msgpack_make_value:
 */
terane_value *
_terane_msgpack_make_value (PyObject *obj)
{
    terane_value *value = NULL;

    value = PyMem_Malloc (sizeof (terane_value));
    if (value == NULL)
        return NULL;

    if (obj == Py_None) {
        value->type = TERANE_MSGPACK_TYPE_NONE;
        return value;
    }
    else if (obj == Py_False) {
        value->type = TERANE_MSGPACK_TYPE_FALSE;
        return value;
    }
    else if (obj == Py_True) {
        value->type = TERANE_MSGPACK_TYPE_TRUE;
        return value;
    }
    else if (PyInt_CheckExact (obj) || PyLong_CheckExact (obj)) {
        PY_LONG_LONG i64;
        unsigned PY_LONG_LONG u64;

        /* most likely the val is within the int64 range */
        i64 = PyLong_AsLongLong (obj);
        if (PyErr_Occurred ()) {
            /* bail if we got an exception other than overflow */
            if (PyErr_ExceptionMatches (PyExc_OverflowError)) {
                PyMem_Free (value);
                return NULL;
            }
            u64 = PyLong_AsUnsignedLongLong (obj);
            /* this shouldn't happen, but semper paratus */
            if (PyErr_Occurred ()) {
                PyMem_Free (value);
                return NULL;
            }
            if (u64 > INT32_MAX) {
                value->type = TERANE_MSGPACK_TYPE_UINT64;
                value->data.u64 = u64;
                return value;
            }
            value->type = TERANE_MSGPACK_TYPE_UINT32;
            value->data.i32 = (uint32_t) u64;
            return value;
        }
        /* obj is negative */
        if (i64 < 0) {
            if (i64 < INT32_MIN) {
                value->type = TERANE_MSGPACK_TYPE_INT64;
                value->data.i64 = i64;
                return value;
            }
            value->type = TERANE_MSGPACK_TYPE_INT32;
            value->data.i32 = (int32_t) i64;
            return value;
        }
        /* obj is positive */
        else {
            if (i64 > INT32_MAX) {
                value->type = TERANE_MSGPACK_TYPE_UINT64;
                value->data.u64 = (uint64_t) i64;
                return value;
            }
            value->type = TERANE_MSGPACK_TYPE_UINT32;
            value->data.i32 = (uint32_t) i64;
            return value;
        }
        return NULL;
    }
    else if (PyFloat_CheckExact (obj)) {
        PyMem_Free (value);
        return NULL;
    }
    else if (PyUnicode_CheckExact (obj)) {
        PyObject *utf8;
        char *buf = NULL;
        Py_ssize_t len = 0;

        utf8 = PyUnicode_AsUTF8String (obj);
        if (utf8 == NULL) {
            PyMem_Free (value);
            return NULL;
        }
        value->type = TERANE_MSGPACK_TYPE_RAW;
        if (PyString_AsStringAndSize (utf8, &buf, &len) < 0) {
            Py_DECREF (utf8);
            PyMem_Free (value);
            return NULL;
        }
        value->data.raw.bytes = PyMem_Malloc (len + 1);
        if (value->data.raw.bytes == NULL) {
            Py_DECREF (utf8);
            PyMem_Free (value);
            return NULL;
        }
        memcpy (value->data.raw.bytes, buf, len);
        value->data.raw.bytes[len] = 0;
        value->data.raw.size = len;
        Py_DECREF (utf8);
        return value;
    }

    PyMem_Free (value);
    PyErr_Format (PyExc_ValueError, "can't dump value of type %s",
        obj->ob_type->tp_name);
    return NULL;
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
 * _msgpack_dump_value:
 */
static int
_msgpack_dump_value (PyObject *obj, struct _buffer *buffer)
{
    terane_value *value = NULL;
    terane_conv conv;
    int ret = -1;

    value = _terane_msgpack_make_value (obj);
    if (value == NULL)
        return -1;

    switch (value->type) {
        /* None */
        case TERANE_MSGPACK_TYPE_NONE:
            if (_buffer_write (buffer, "\xc0", 1) < 0)
                goto error;
            break;
        /* False */
        case TERANE_MSGPACK_TYPE_FALSE: 
            if (_buffer_write (buffer, "\xc2", 1) < 0)
                goto error;
            break;
        /* True */
        case TERANE_MSGPACK_TYPE_TRUE:
            if (_buffer_write (buffer, "\xc3", 1) < 0)
                goto error;
            break;
        /* int 64 */
        case TERANE_MSGPACK_TYPE_INT64:
            if (value->data.i64 >= 0 || value->data.i64 < -2147483648LL) {
                PyErr_Format (PyExc_ValueError, "int64 value is out of range (%lld)",
                    value->data.i64);
                goto error;
            }
            if (_buffer_write (buffer, "\xd3", 1) < 0)
                goto error;
            conv.i64 = HTONQ(value->data.i64);
            if (_buffer_write (buffer, (char *) &conv.i64, 8) < 0)
                goto error;
            break;
        case TERANE_MSGPACK_TYPE_INT32:
            if (value->data.i32 >= 0) {
                PyErr_Format (PyExc_ValueError, "int32 value is out of range (%ld)",
                    (long int) value->data.i32);
                goto error;
            }
            /* negative fixnum */
            if (value->data.i32 >= -32) {
                conv.i8 = (int8_t) value->data.i32;
                if (_buffer_write (buffer, (char *) &conv.i8, 1) < 0)
                    goto error;
            }
            /* int 8 */
            else if (value->data.i32 >= -128) {
                if (_buffer_write (buffer, "\xd0", 1) < 0)
                    goto error;
                conv.i8 = (int8_t) value->data.i32;
                if (_buffer_write (buffer, (char *) &conv.i8, 1) < 0)
                    goto error;
            }
            /* int 16 */
            else if (value->data.i32 >= 32768) {
                if (_buffer_write (buffer, "\xd1", 1) < 0)
                    goto error;
                conv.i16 = HTONS((int16_t) value->data.i32);
                if (_buffer_write (buffer, (char *) &conv.i16, 2) < 0)
                    goto error;
            }
            /* int 32 */
            else {
                if (_buffer_write (buffer, "\xd2", 1) < 0)
                    goto error;
                conv.i32 = HTONL(value->data.i32);
                if (_buffer_write (buffer, (char *) &conv.i32, 4) < 0)
                    goto error;
            }
            break;
        case TERANE_MSGPACK_TYPE_UINT32:
            /* positive fixnum */
            if (value->data.u32 < 128) {
                conv.u8 = (uint8_t) value->data.u32;
                if (_buffer_write (buffer, (char *) &conv.u8, 1) < 0)
                    goto error;
            }
            /* uint 8 */
            else if (value->data.u32 < 256) {
                if (_buffer_write (buffer, "\xcc", 1) < 0)
                    goto error;
                conv.u8 = (uint8_t) value->data.u32;
                if (_buffer_write (buffer, (char *) &conv.u8, 1) < 0)
                    goto error;
            }
            /* uint 16 */
            else if (value->data.i32 < 32768) {
                if (_buffer_write (buffer, "\xcd", 1) < 0)
                    goto error;
                conv.u16 = HTONS((uint16_t) value->data.u32);
                if (_buffer_write (buffer, (char *) &conv.u16, 2) < 0)
                    goto error;
            }
            /* uint 32 */
            else {
                if (_buffer_write (buffer, "\xce", 1) < 0)
                    goto error;
                conv.u32 = HTONL(value->data.u32);
                if (_buffer_write (buffer, (char *) &conv.u32, 4) < 0)
                    goto error;
            }
            break;
        /* uint 64 */
        case TERANE_MSGPACK_TYPE_UINT64:
            if (value->data.u64 < 2147483648LL) {
                PyErr_Format (PyExc_ValueError, "int64 value is out of range (%llu)",
                    value->data.u64);
                goto error;
            }
            if (_buffer_write (buffer, "\xcf", 1) < 0)
                return -1;
            conv.u64 = HTONQ(value->data.u64);
            if (_buffer_write (buffer, (char *) &conv.u64, 8) < 0)
                return -1;
            break;
        /* double */
        case TERANE_MSGPACK_TYPE_DOUBLE:
            goto error;
        case TERANE_MSGPACK_TYPE_RAW:
            /* fixraw */
            if (value->data.raw.size < 32) {
                conv.u8 = ((uint8_t) value->data.raw.size) | 0xa0;
                if (_buffer_write (buffer, (char *) &conv.u8, 1) < 0)
                    goto error;
                if (_buffer_write (buffer, value->data.raw.bytes, value->data.raw.size) < 0)
                    goto error;
            }
            /* raw 16 */
            else if (value->data.raw.size < 32768) {
                if (_buffer_write (buffer, "\xda", 1) < 0)
                    goto error;
                conv.u16 = HTONS((uint16_t) value->data.raw.size);
                if (_buffer_write (buffer, (char *) &conv.u16, 2) < 0)
                    goto error;
                if (_buffer_write (buffer, value->data.raw.bytes, value->data.raw.size) < 0)
                    goto error;
            }
            /* raw 32 */
            else {
                if (_buffer_write (buffer, "\xdb", 1) < 0)
                    goto error;
                conv.u32 = HTONL(value->data.raw.size);
                if (_buffer_write (buffer, (char *) &conv.u32, 4) < 0)
                    goto error;
                if (_buffer_write (buffer, value->data.raw.bytes, value->data.raw.size) < 0)
                    goto error;
            }
            break;
        /* unknown type */
        default:
            PyErr_Format (PyExc_ValueError, "can't dump value of type %s",
                obj->ob_type->tp_name);
            goto error;
    }
    /* if we reach here, then we have succeeded in writing a value */
    ret = 1;

error:
    if (value)
        _terane_msgpack_free_value (value);
    return ret;
}

/*
 * _msgpack_dump_object:
 */
static int
_msgpack_dump_object (PyObject *obj, struct _buffer *buffer)
{
    uint8_t u8;
    uint16_t u16;
    uint32_t u32;
    PyObject **items, *key, *value;
    Py_ssize_t size, i = 0;

    if (obj == NULL)
        return 0;

    /* list */
    else if (PyTuple_CheckExact (obj)) {
        size = PySequence_Fast_GET_SIZE (obj);
        items = PySequence_Fast_ITEMS (obj);

        /* fix array */
        if (size < 16) {
            u8 = (uint8_t) size | 0x90;
            if (_buffer_write (buffer, (char *) &u8, 1) < 0)
                return -1;
        }
        /* array 16 */
        else if (size < 65536) {
            u8 = 0xdc;
            u16 = (uint16_t) size;
            u16 = HTONS(u16);
            if (_buffer_write (buffer, (char *) &u8, 1) < 0)
                return -1;
            if (_buffer_write (buffer, (char *) &u16, 2) < 0)
                return -1;
        }
        /* array 32 */
        else {
            u8 = 0xdc;
            u32 = (uint32_t) size;
            u32 = HTONL(u32);
            if (_buffer_write (buffer, (char *) &u8, 1) < 0)
                return -1;
            if (_buffer_write (buffer, (char *) &u32, 4) < 0)
                return -1;
        }
        /* write each tuple item */
        for (i = 0; i < size; i++) {
            if (_msgpack_dump_object (items[i], buffer) < 0)
                return -1;
        }
        return 1;
    }
    /* dict */
    else if (PyDict_CheckExact (obj)) {
        size = PyDict_Size (obj);

        /* fix map */
        if (size < 16) {
            u8 = ((uint8_t) size) | 0x80;
            if (_buffer_write (buffer, (char *) &u8, 1) < 0)
                return -1;
        }
        /* map 16 */
        else if (size < 65536) {
            u8 = 0xde;
            u16 = (uint16_t) size;
            u16 = HTONS(u16);
            if (_buffer_write (buffer, (char *) &u8, 1) < 0)
                return -1;
            if (_buffer_write (buffer, (char *) &u16, 2) < 0)
                return -1;
        }
        /* map 32 */
        else {
            u8 = 0xdf;
            u32 = (uint32_t) size;
            u32 = HTONL(u32);
            if (_buffer_write (buffer, (char *) &u8, 1) < 0)
                return -1;
            if (_buffer_write (buffer, (char *) &u32, 4) < 0)
                return -1;
        }

        /* write each dict item */
        while (PyDict_Next (obj, &i, &key, &value)) {
            if (_msgpack_dump_object (key, buffer) < 0)
                return -1;
            if (_msgpack_dump_object (value, buffer) < 0)
                return -1;
        }
        return 1;
    }

    return _msgpack_dump_value (obj, buffer);
}

/*
 * _terane_msgpack_dump: serialize a python object into a buffer.
 */
int
_terane_msgpack_dump (PyObject *obj, char **buf, uint32_t *len)
{
    struct _buffer buffer;
    PyObject *seq, **items;
    Py_ssize_t nitems, i;

    memset (&buffer, 0, sizeof (buffer));

    if (PyList_CheckExact (obj)) {
        seq = PySequence_Fast (obj, NULL);
        nitems = PySequence_Fast_GET_SIZE (seq);
        items = PySequence_Fast_ITEMS (seq);
        for (i = 0; i < nitems; i++) {
            if (_msgpack_dump_object (items[i], &buffer) < 0)
                goto error;
        }
    }
    else {
        if (_msgpack_dump_object (obj, &buffer) < 0)
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

/*
 * _terane_msgpack_free_value:
 */
void
_terane_msgpack_free_value (terane_value *value)
{
    assert (value != NULL);

    if (value->type == TERANE_MSGPACK_TYPE_RAW) {
        if (value->data.raw.bytes != NULL)
            PyMem_Free (value->data.raw.bytes);
    }
    PyMem_Free (value);
}
