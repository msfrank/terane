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
_msgpack_load_object (msgpack_object *obj, PyObject **dest)
{
    PyObject *dict = NULL;
    PyObject *list = NULL;
    PyObject *key = NULL;
    PyObject *value = NULL;
    int i;

    switch (obj->type) {
        case MSGPACK_OBJECT_NIL:
            *dest = Py_None;
            Py_INCREF (Py_None);
            break;
        case MSGPACK_OBJECT_BOOLEAN:
            if (obj->via.boolean)
                *dest = Py_True;
            else
                *dest = Py_False;
            Py_INCREF (*dest);
            break;
        case MSGPACK_OBJECT_POSITIVE_INTEGER:
            if (obj->via.u64 > LONG_MAX)
                *dest = PyLong_FromUnsignedLongLong (obj->via.u64);
            else
                *dest = PyInt_FromSize_t ((size_t) obj->via.u64);
            if (*dest == NULL)
                return -1;
            break;
        case MSGPACK_OBJECT_NEGATIVE_INTEGER:
            if (obj->via.i64 < LONG_MIN)
                *dest = PyLong_FromLongLong (obj->via.i64);
            else
                *dest = PyInt_FromSsize_t ((Py_ssize_t) obj->via.i64);
            if (*dest == NULL)
                return -1;
            break;
        case MSGPACK_OBJECT_DOUBLE:
            *dest = PyFloat_FromDouble (obj->via.dec);
            if (*dest == NULL)
                return -1;
            break;
        case MSGPACK_OBJECT_RAW:
            *dest = PyUnicode_DecodeUTF8 (obj->via.raw.ptr, obj->via.raw.size, "strict");
            if (*dest == NULL)
                return -1;
            break;
        case MSGPACK_OBJECT_ARRAY:
            list = PyList_New (obj->via.array.size);
            if (list == NULL)
                goto error;
            for (i = 0; i < obj->via.array.size; i++) {
                if (_msgpack_load_object (&obj->via.array.ptr[i], &value) < 0)
                    goto error;
                if (PyList_SET_ITEM (list, i, value) < 0)
                    goto error;
                value = NULL;
            }
            *dest = list;
            break;
        case MSGPACK_OBJECT_MAP:
            dict = PyDict_New ();
            if (dict == NULL)
                goto error;
            for (i = 0; i < obj->via.map.size; i++) {
                if (_msgpack_load_object (&obj->via.map.ptr[i].key, &key) < 0)
                    goto error;
                if (_msgpack_load_object (&obj->via.map.ptr[i].val, &value) < 0)
                    goto error;
                if (PyDict_SetItem (dict, key, value) < 0)
                    goto error;
                Py_DECREF (key);
                Py_DECREF (value);
                key = NULL;
                value = NULL;
            }
            *dest = dict;
            break;
        default:
            PyErr_Format (PyExc_ValueError, "can't deserialize object type %i", obj->type);
            return -1;
    }
    return 0;

error:
    if (list)
        Py_DECREF (list);
    if (dict)
        Py_DECREF (dict);
    if (key)
        Py_DECREF (key);
    if (value)
        Py_DECREF (value);
    return -1;
}

/*
 * _terane_msgpack_load: deserialize a buffer into a python object.
 */
int
_terane_msgpack_load (const char *buf, uint32_t len, PyObject **dest)
{
    msgpack_unpacker unpacker;
    msgpack_unpacked unpacked;
    PyObject *obj = NULL, *list = NULL;

    /* initialize the unpacker */
    msgpack_unpacker_init (&unpacker, MSGPACK_UNPACKER_INIT_BUFFER_SIZE);
    msgpack_unpacker_reserve_buffer (&unpacker, len);
    memcpy (msgpack_unpacker_buffer (&unpacker), buf, len);
    msgpack_unpacker_buffer_consumed (&unpacker, len);

    /* unpack each item */
    msgpack_unpacked_init (&unpacked);
    while (msgpack_unpacker_next (&unpacker, &unpacked)) {
        if (obj == NULL && list == NULL) {
            if (_msgpack_load_object (&unpacked.data, &obj) < 0)
                goto error;
        }
        else {
            if (list == NULL) {
                list = PyList_New (0);
                if (list == NULL)
                    goto error;
            }
            if (obj != NULL) {
                if (PyList_Append (list, obj) < 0)
                    goto error;
                Py_DECREF (obj);
                obj = NULL;
            }
            if (_msgpack_load_object (&unpacked.data, &obj) < 0)
                goto error;
            if (PyList_Append (list, obj) < 0)
                goto error;
            Py_DECREF (obj);
            obj = NULL;
        }
    }
    msgpack_unpacker_destroy (&unpacker);

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
    msgpack_unpacker_destroy (&unpacker);
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
    const char *str = NULL;
    int len;
    PyObject *obj = NULL;

    /* parse parameters */
    if (!PyArg_ParseTuple (args, "s#", &str, &len))
        return NULL;
    if (_terane_msgpack_load (str, (uint32_t) len, &obj) < 0)
        return NULL;
    return obj;
}
