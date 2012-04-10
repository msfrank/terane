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
 * _json_dump_list: marshal a python list as a JSON string into the specified buffer.
 */
static _buffer *
_json_dump_list (_buffer *buffer, PyObject *list)
{
    PyObject *value = NULL;
    Py_ssize_t list_pos = 0, list_len = 0;
    int first_iteration = 1;

    list_len = PySequence_Size (list);

    if (_buffer_append_c_str (buffer, "[ ", 2) == NULL)
        return NULL;
    for (list_pos = 0; list_pos < list_len; list_pos++) {
        value = PyList_GetItem (list, list_pos);
        if (value == NULL) /* shouldn't ever get this */
            return NULL;
        if (first_iteration)
            first_iteration = 0;
        else if (_buffer_append_c_str (buffer, ", ", 2) == NULL)
            return NULL;
        if (_json_dump_object (buffer, value) == NULL)
            return NULL;
    }
    if (_buffer_append_c_str (buffer, " ]", 2) == NULL)
        return NULL;

    return buffer;
}

/*
 * _json_dump_dict: marshal a python dict as a JSON string into the specified buffer.
 */
static _buffer *
_json_dump_dict (_buffer *buffer, PyObject *dict)
{
    PyObject *key = NULL, *value = NULL;
    Py_ssize_t dict_pos;
    int first_iteration = 1;

    if (_buffer_append_c_str (buffer, "{ ", 2) == NULL)
        return NULL;
    while (PyDict_Next (dict, &dict_pos, &key, &value)) {
        if (!PyString_Check (key))
            return (_buffer *) PyErr_Format (PyExc_TypeError,
                "dict key must be of type str, not '%s'", key->ob_type->tp_name);
        if (first_iteration)
            first_iteration = 0;
        else if (_buffer_append_c_str (buffer, ", ", 2) == NULL)
            return NULL;
        if (_buffer_append_c_str (buffer, "\"", 1) == NULL)
            return NULL;
        if (_buffer_append_py_str (buffer, key) == NULL)
            return NULL;
        if (_buffer_append_c_str (buffer, "\": ", 3) == NULL)
            return NULL;
        if (_json_dump_object (buffer, value) == NULL)
            return NULL;
    }
    if (_buffer_append_c_str (buffer, " }", 2) == NULL)
        return NULL;

    return buffer;
}

/*
 * _json_dump_object: marshal a python object as a JSON string into the specified buffer.
 */
static _buffer *
_json_dump_object (_buffer *buffer, PyObject *obj)
{
    /* object is None */
    if (Py_None == obj)
        return _buffer_append_c_str (buffer, "null", 4);
    /* object is True or False */
    else if (PyBool_Check (obj)) {
        if (obj == Py_False)
            return _buffer_append_c_str (buffer, "false", 5);
        else
            return _buffer_append_c_str (buffer, "true", 4);
    }
    /* object is of type int or long or float */
    else if (PyInt_Check (obj) || PyLong_Check (obj) || PyFloat_Check (obj)) {
        PyObject *str = PyObject_Str (obj);
        if (str == NULL)
            return NULL;
        if (_buffer_append_py_str (buffer, str) == NULL)
            return NULL;
        Py_DECREF (str);
        return buffer;
    }
    /* object is of type str */
    else if (PyString_Check (obj)) {
        if (_buffer_append_c_str (buffer, "\"", 1) == NULL)
            return NULL;
        if (_buffer_append_py_str (buffer, obj) == NULL)
            return NULL;
        if (_buffer_append_c_str (buffer, "\"", 1) == NULL)
            return NULL;
        return buffer;
    }
    /* object is of type unicode */
    else if (PyUnicode_Check (obj)) {
        _buffer_append_c_str (buffer, "\"", 1);
        _buffer_append_py_unicode (buffer, obj);
        _buffer_append_c_str (buffer, "\"", 1);
        return buffer;
    }
    /* object is of type list or tuple */
    else if (PyList_Check (obj) || PyTuple_Check (obj))
        return _json_dump_list (buffer, obj);
    /* object is of type dict */
    else if (PyDict_Check (obj))
        return _json_dump_dict (buffer, obj);
    /* otherwise, we don't know how to dump this object type */
    PyErr_Format (PyExc_TypeError, "can't dump object of type '%s'", obj->ob_type->tp_name);
    return NULL;
}

/*
 * 
 */
int
terane_json_dump (PyObject *obj, char **data, Py_ssize_t *len)
{
    _buffer *buffer;

    assert (data != NULL);
    assert (len != NULL);

    buffer = _buffer_new ();
    if (_json_dump_object (buffer, obj) == NULL) {
        _buffer_free (buffer, 1);
        return -1;
    }
    *data = buffer->data;
    *len = buffer->len;
    _buffer_free (buffer, 0);
    return 0;
}
