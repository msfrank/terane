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

#define CONTAINS_BYTES(b,l,p,n)     (*((const char **)p) + n <= (const char *)b + l)

#define NTOHS(s)        ntohs(s)
#define NTOHL(l)        ntohl(l)
#define NTOHQ(q)        be64toh(q)

/*
 * 
 */
int
_terane_msgpack_load_value (char *          buf,
                            uint32_t        len,
                            char **         pos, 
                            terane_value *  val)
{
    unsigned char type;
    terane_conv *conv = NULL;

    assert (buf != NULL);
    assert (pos != NULL && (*pos == NULL || *pos >= buf));
    assert (val != NULL);

    /* if we have reached the end of the buffer, return 0 */
    if (buf + len <= *pos)
        return 0;
    /* if *pos is NULL, then start reading from the beginning */
    if (*pos == NULL)
        *pos = buf;

    /* get the type byte and increment the position */
    type = (unsigned char) **pos;
    *pos += 1;

    switch (type) {
        /* nil */
        case 0xc0:
            val->type = TERANE_MSGPACK_TYPE_NONE;
            return 1;
        /* false */
        case 0xc2:
            val->type = TERANE_MSGPACK_TYPE_FALSE;
            return 1;
        /* true */
        case 0xc3:
            val->type = TERANE_MSGPACK_TYPE_TRUE;
            return 1;
        /* float */
        case 0xca:
            if (!CONTAINS_BYTES(buf, len, pos, 4))
                return -1;
            *pos += 4;
            return -1;
        /* double */
        case 0xcb:
            if (!CONTAINS_BYTES(buf, len, pos, 8))
                return -1;
            *pos += 8;
            return -1;
        /* uint 8 */
        case 0xcc:
            if (!CONTAINS_BYTES(buf, len, pos, 1))
                return -1;
            conv = (terane_conv *) *pos;
            val->type = TERANE_MSGPACK_TYPE_UINT32;
            val->data.u32 = conv->u8;
            *pos += 1;
            return 1;
        /* uint 16 */
        case 0xcd:
            if (!CONTAINS_BYTES(buf, len, pos, 2))
                return -1;
            conv = (terane_conv *) *pos;
            val->type = TERANE_MSGPACK_TYPE_UINT32;
            val->data.u32 = NTOHS(conv->u16);
            *pos += 2;
            return 1;
        /* uint 32 */
        case 0xce:
            if (!CONTAINS_BYTES(buf, len, pos, 4))
                return -1;
            conv = (terane_conv *) *pos;
            val->type = TERANE_MSGPACK_TYPE_UINT32;
            val->data.u32 = NTOHL(conv->u32);
            *pos += 4;
            return 1;
        /* uint 64 */
        case 0xcf:
            if (!CONTAINS_BYTES(buf, len, pos, 8))
                return -1;
            conv = (terane_conv *) *pos;
            val->type = TERANE_MSGPACK_TYPE_UINT64;
            val->data.u64 = NTOHQ(conv->u64);
            *pos += 8;
            return 1;
        /* int 8 */
        case 0xd0:
            if (!CONTAINS_BYTES(buf, len, pos, 1))
                return -1;
            conv = (terane_conv *) *pos;
            val->type = TERANE_MSGPACK_TYPE_INT32;
            val->data.i32 = conv->i8;
            *pos += 1;
            return 1;
        /* int 16 */
        case 0xd1:
            if (!CONTAINS_BYTES(buf, len, pos, 2))
                return -1;
            conv = (terane_conv *) *pos;
            val->type = TERANE_MSGPACK_TYPE_INT32;
            val->data.i32 = NTOHS(conv->i16);
            *pos += 2;
            return 1;
        /* int 32 */
        case 0xd2:
            if (!CONTAINS_BYTES(buf, len, pos, 4))
                return -1;
            conv = (terane_conv *) *pos;
            val->type = TERANE_MSGPACK_TYPE_INT32;
            val->data.i32 = NTOHL(conv->i32);
            *pos += 4;
            return 1;
        /* int 64 */
        case 0xd3:
            if (!CONTAINS_BYTES(buf, len, pos, 8))
                return -1;
            conv = (terane_conv *) *pos;
            val->type = TERANE_MSGPACK_TYPE_INT64;
            val->data.i64 = NTOHQ(conv->i64);
            *pos += 8;
            return 1;
        /* raw 16  */
        case 0xda:
            if (!CONTAINS_BYTES(buf, len, pos, 2))
                return -1;
            conv = (terane_conv *) *pos;
            val->type = TERANE_MSGPACK_TYPE_RAW;
            val->data.raw.size = (uint32_t) NTOHS(conv->u16);
            *pos += 2;
            if (!CONTAINS_BYTES(buf, len, pos, val->data.raw.size))
                return -1;
            val->data.raw.bytes = *pos;
            *pos += val->data.raw.size;
            return 1;
        /* raw 32 */
        case 0xdb:
            if (!CONTAINS_BYTES(buf, len, pos, 4))
                return -1;
            conv = (terane_conv *) *pos;
            val->type = TERANE_MSGPACK_TYPE_RAW;
            val->data.raw.size = NTOHL(conv->u32);
            *pos += 4;
            if (!CONTAINS_BYTES(buf, len, pos, val->data.raw.size))
                return -1;
            val->data.raw.bytes = *pos;
            *pos += val->data.raw.size;
            return 1;
        /* unknown type */
        default:
            break;
    }
    /* Positive FixNum */
    if (!(type & 0x80)) {
        val->type = TERANE_MSGPACK_TYPE_UINT32;
        val->data.u32 = type & 0x7f;
        return 1;
    }
    /* Negative FixNum */
    if ((type & 0xe0) == 0xe0) {
        val->type = TERANE_MSGPACK_TYPE_INT32;
        val->data.i32 = (int8_t) type;
        return 1;
    }
    /* FixRaw */
    if ((type & 0xe0) == 0xa0) {
        val->type = TERANE_MSGPACK_TYPE_RAW;
        val->data.raw.size = type & 0x1f;
        if (!CONTAINS_BYTES(buf, len, pos, val->data.raw.size))
            return -1;
        val->data.raw.bytes = *pos;
        *pos += val->data.raw.size;
        return 1;
    }

    /* rewind the pos by one byte */
    *pos -= 1;

    /* -2 means the data type is unknown */
    return -2;
}

/* forward declaration */
static int
_msgpack_load_object (char *        buf,
                      uint32_t      len,
                      char **       pos, 
                      PyObject **   obj);

/*
 * _msgpack_load_tuple:
 */
static PyObject *
_msgpack_load_tuple (char *       buf,
                     uint32_t     len,
                     char **      pos,
                     Py_ssize_t   size)
{
    PyObject *list = NULL, *item = NULL;
    int i;

    list = PyTuple_New (size);
    if (list == NULL)
        return NULL;
    for (i = 0; i < size; i++) {
        if (_msgpack_load_object (buf, len, pos, &item) < 0)
            goto error;
        PyTuple_SET_ITEM (list, i, item);
        item = NULL;
    }
    return list;
    
error:
    if (list)
        Py_DECREF (list);
    if (item)
        Py_DECREF (item);
    return NULL;
}

/*
 * _msgpack_load_dict:
 */
static PyObject *
_msgpack_load_dict (char *        buf,
                    uint32_t      len,
                    char **       pos,
                    Py_ssize_t    size)
{
    PyObject *dict = NULL, *key = NULL, *value = NULL;
    int i;

    dict = PyDict_New ();
    if (dict == NULL)
        return NULL;
    for (i = 0; i < size; i++) {
        if (_msgpack_load_object (buf, len, pos, &key) < 0)
            goto error;
        if (_msgpack_load_object (buf, len, pos, &value) < 0)
            goto error;
        if (PyDict_SetItem (dict, key, value) < 0)
            goto error;
        Py_DECREF (key);
        Py_DECREF (value);
        key = NULL;
        value = NULL;
    }
    return dict;
    
error:
    if (dict)
        Py_DECREF (dict);
    if (key)
        Py_DECREF (key);
    if (value)
        Py_DECREF (value);
    return NULL;
}

/*
 * _msgpack_load_object:
 */
static int
_msgpack_load_object (char *        buf,
                      uint32_t      len,
                      char **       pos, 
                      PyObject **   obj)
{
    terane_value val;
    int ret;
    unsigned char type;
    terane_conv *conv;

    ret = _terane_msgpack_load_value (buf, len, pos, &val);
    /* there was an error loading the value, give up */
    if (ret == -1)
        return -1;
    /* we have reached the end of the buffer */
    if (ret == 0)
        return 0;
    /*  a value was returned, convert it to a python object */
    if (ret > 0) {
        switch (val.type) {
            case TERANE_MSGPACK_TYPE_NONE:
                *obj = Py_None;
                Py_INCREF (Py_None);
                return 1;
            case TERANE_MSGPACK_TYPE_FALSE:
                *obj = Py_False;
                Py_INCREF (*obj);
                return 1;
            case TERANE_MSGPACK_TYPE_TRUE:
                *obj = Py_True;
                Py_INCREF (*obj);
                return 1;
            case TERANE_MSGPACK_TYPE_UINT32:
                *obj = PyInt_FromSize_t ((size_t) val.data.u32);
                break;
            case TERANE_MSGPACK_TYPE_INT32:
                *obj = PyInt_FromSsize_t ((ssize_t) val.data.i32);
                break;
            case TERANE_MSGPACK_TYPE_UINT64:
                *obj = PyLong_FromUnsignedLongLong (val.data.u64);
                break;
            case TERANE_MSGPACK_TYPE_INT64:
                *obj = PyLong_FromLongLong (val.data.i64);
                break;
            case TERANE_MSGPACK_TYPE_DOUBLE:
                *obj = PyFloat_FromDouble (val.data.f64);
                break;
            case TERANE_MSGPACK_TYPE_RAW:
                *obj = PyUnicode_DecodeUTF8 (val.data.raw.bytes, val.data.raw.size, "strict");
                break;
        }
        if (*obj == NULL)
            return -1;
        return 1;
    }

    /* otherwise load a complex type */
    type = (unsigned char) **pos;
    *pos += 1;

    switch (type) {
        /* array 16 */
        case 0xdc:
            if (!CONTAINS_BYTES(buf, len, pos, 2))
                return -1;
            conv = (terane_conv *) *pos;
            *pos += 2;
            *obj = _msgpack_load_tuple (buf, len, pos, (Py_ssize_t) NTOHS(conv->u16));
            if (*obj == NULL)
                return -1;
            return 1;
        /* array 32 */
        case 0xdd:
            if (!CONTAINS_BYTES(buf, len, pos, 4))
                return -1;
            conv = (terane_conv *) *pos;
            *pos += 4;
            *obj = _msgpack_load_tuple (buf, len, pos, (Py_ssize_t) NTOHL(conv->u32));
            if (*obj == NULL)
                return -1;
            return 1;
        /* map 16 */
        case 0xde:
            if (!CONTAINS_BYTES(buf, len, pos, 2))
                return -1;
            conv = (terane_conv *) *pos;
            *pos += 2;
            *obj = _msgpack_load_dict (buf, len, pos, (Py_ssize_t) NTOHS(conv->u16));
            if (*obj == NULL)
                return -1;
            return 1;
        /* map 32 */
        case 0xdf:
            if (!CONTAINS_BYTES(buf, len, pos, 4))
                return -1;
            conv = (terane_conv *) *pos;
            *pos += 4;
            *obj = _msgpack_load_dict (buf, len, pos, (Py_ssize_t) NTOHL(conv->u32));
            if (*obj == NULL)
                return -1;
            return 1;
        /* fall through */
        default:
            break;
    }

    /* FixArray */
    if ((type & 0xf0) == 0x90) {
        *obj = _msgpack_load_tuple (buf, len, pos, type & 0x0f);
        if (*obj == NULL)
                return -1;
        return 1;
    }
    /* FixMap */
    if ((type & 0xf0) == 0x80) {
        *obj = _msgpack_load_dict (buf, len, pos, type & 0x0f);
        if (*obj == NULL)
                return -1;
        return 1;
    }

    /* we don't know how to handle this type */
    PyErr_Format (PyExc_ValueError, "unable to load data with type %x", (int) type);
    return -1;
}

/*
 * _terane_msgpack_load: deserialize a buffer into a python object.
 */
int
_terane_msgpack_load (char *buf, uint32_t len, PyObject **dest)
{
    PyObject *obj = NULL, *next = NULL, *list = NULL;
    char *pos = NULL;
    int ret = 1;

    /* unpack each item */
    while (ret > 0) {
        /* this is the first item */
        if (obj == NULL && list == NULL)
            ret = _msgpack_load_object (buf, len, &pos, &obj);
        else {
            /* load the next item */
            ret = _msgpack_load_object (buf, len, &pos, &next);
            /* if there are no more items, or error, then break */
            if (ret <= 0)
                break;
            /* this is the second item */
            if (list == NULL) {
                list = PyList_New (0);
                if (list == NULL)
                    goto error;
            }
            /* append the previous item to the list */
            if (obj != NULL) {
                if (PyList_Append (list, obj) < 0)
                    goto error;
                Py_DECREF (obj);
                obj = NULL;
            }
            /* append the current item to the list */
            if (PyList_Append (list, next) < 0)
                goto error;
            Py_DECREF (next);
            next = NULL;
        }
    }
    if (ret < 0)
        goto error;
    /* return the deserialized object */
    if (list)
        *dest = list;
    else
        *dest = obj;
    return 0;

error:
    if (list)
        Py_DECREF (list);
    if (obj)
        Py_DECREF (obj);
    if (next)
        Py_DECREF (next);
    return -1;
}

/*
 * terane_msgpack_load: 
 *
 * callspec: msgpack_load(string)
 * parameters:
 *   string (str):
 * returns:
 * exceptions:
 *   ValueError:
 */
PyObject *
terane_msgpack_load (PyObject *self, PyObject *args)
{
    char *str = NULL;
    int len;
    PyObject *obj = NULL;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "s#", &str, &len))
        return NULL;
    if (_terane_msgpack_load (str, (uint32_t) len, &obj) < 0)
        return NULL;
    return obj;
}
