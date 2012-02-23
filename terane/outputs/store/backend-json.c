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

typedef struct {
    char *buffer;   /* allocated buffer */
    size_t length;  /* length of the string in the buffer, not including the null terminator */
} _json_buffer;

static _json_buffer *
_json_buffer_new (void)
{
    _json_buffer *buffer;

    buffer = PyMem_Malloc (sizeof (_json_buffer));
    if (buffer == NULL)
        return (_json_buffer *) PyErr_NoMemory ();
    memset (buffer, 0, sizeof (_json_buffer));
    return buffer;
}

void
_json_buffer_free (_json_buffer *buffer)
{
    assert (buffer != NULL);

    if (buffer->buffer)
        PyMem_Free (buffer->buffer);
    PyMem_Free (buffer);
}

static _json_buffer *
_json_buffer_push_c_str (_json_buffer *buffer, const char *c_str, Py_ssize_t len)
{
    char *buf;

    assert (buffer != NULL);
    assert (c_str != NULL);
    assert (len >= 0);
    buf = PyMem_Realloc (buffer->buffer, buffer->length + len + 1);
    if (buf == NULL)
        return (_json_buffer *) PyErr_NoMemory ();
    buffer->buffer = buf;
    strncpy (&buffer->buffer[buffer->length], c_str, len);
    buffer->length = buffer->length + len;
    buffer->buffer[buffer->length] = '\0';
    return buffer;
}

static _json_buffer *
_json_buffer_push_py_str (_json_buffer *buffer, PyObject *py_str)
{
    char *c_str = NULL;
    Py_ssize_t len = 0;

    assert (buffer != NULL);
    assert (py_str != NULL);
    if (PyString_AsStringAndSize (py_str, &c_str, &len) < 0)
        return NULL;
    return _json_buffer_push_c_str (buffer, c_str, len);
}

static _json_buffer *
_json_buffer_push_py_unicode (_json_buffer *buffer, PyObject *py_unicode)
{
    PyObject *py_str = NULL;

    assert (buffer != NULL);
    assert (py_unicode != NULL);

    py_str = PyUnicode_AsUTF8String (py_unicode);
    if (py_str == NULL)
        return NULL;
    buffer = _json_buffer_push_py_str (buffer, py_str);
    Py_DECREF (py_str);
    return buffer;
}

/* forward function declaration */
static _json_buffer *_json_dump_object (_json_buffer *buffer, PyObject *obj);

static _json_buffer *
_json_dump_list (_json_buffer *buffer, PyObject *list)
{
    PyObject *value = NULL;
    Py_ssize_t list_pos = 0, list_len = 0;
    int first_iteration = 1;

    list_len = PySequence_Size (list);

    if (_json_buffer_push_c_str (buffer, "[ ", 2) == NULL)
        return NULL;
    for (list_pos = 0; list_pos < list_len; list_pos++) {
        value = PyList_GetItem (list, list_pos);
        if (value == NULL) /* shouldn't ever get this */
            return NULL;
        if (first_iteration)
            first_iteration = 0;
        else if (_json_buffer_push_c_str (buffer, ", ", 2) == NULL)
            return NULL;
        if (_json_dump_object (buffer, value) == NULL)
            return NULL;
    }
    if (_json_buffer_push_c_str (buffer, " ]", 2) == NULL)
        return NULL;

    return buffer;
}

static _json_buffer *
_json_dump_dict (_json_buffer *buffer, PyObject *dict)
{
    PyObject *key = NULL, *value = NULL;
    Py_ssize_t dict_pos;
    int first_iteration = 1;

    if (_json_buffer_push_c_str (buffer, "{ ", 2) == NULL)
        return NULL;
    while (PyDict_Next (dict, &dict_pos, &key, &value)) {
        if (!PyString_Check (key))
            return (_json_buffer *) PyErr_Format (PyExc_TypeError,
                "dict key must be of type str, not '%s'", key->ob_type->tp_name);
        if (first_iteration)
            first_iteration = 0;
        else if (_json_buffer_push_c_str (buffer, ", ", 2) == NULL)
            return NULL;
        if (_json_buffer_push_c_str (buffer, "\"", 1) == NULL)
            return NULL;
        if (_json_buffer_push_py_str (buffer, key) == NULL)
            return NULL;
        if (_json_buffer_push_c_str (buffer, "\": ", 3) == NULL)
            return NULL;
        if (_json_dump_object (buffer, value) == NULL)
            return NULL;
    }
    if (_json_buffer_push_c_str (buffer, " }", 2) == NULL)
        return NULL;

    return buffer;
}

/*
 *
 */
static _json_buffer *
_json_dump_object (_json_buffer *buffer, PyObject *obj)
{
    /* object is None */
    if (Py_None == obj)
        return _json_buffer_push_c_str (buffer, "null", 4);
    /* object is True or False */
    else if (PyBool_Check (obj)) {
        if (obj == Py_False)
            return _json_buffer_push_c_str (buffer, "false", 5);
        else
            return _json_buffer_push_c_str (buffer, "true", 4);
    }
    /* object is of type int or long or float */
    else if (PyInt_Check (obj) || PyLong_Check (obj) || PyFloat_Check (obj)) {
        PyObject *str = PyObject_Str (obj);
        if (str == NULL)
            return NULL;
        if (_json_buffer_push_py_str (buffer, str) == NULL)
            return NULL;
        Py_DECREF (str);
        return buffer;
    }
    /* object is of type str */
    else if (PyString_Check (obj)) {
        if (_json_buffer_push_c_str (buffer, "\"", 1) == NULL)
            return NULL;
        if (_json_buffer_push_py_str (buffer, obj) == NULL)
            return NULL;
        if (_json_buffer_push_c_str (buffer, "\"", 1) == NULL)
            return NULL;
        return buffer;
    }
    /* object is of type unicode */
    else if (PyUnicode_Check (obj)) {
        _json_buffer_push_c_str (buffer, "\"", 1);
        _json_buffer_push_py_unicode (buffer, obj);
        _json_buffer_push_c_str (buffer, "\"", 1);
        return buffer;
    }
    /* object is of type unicode */
    else if (PyList_Check (obj) || PyTuple_Check (obj))
        return _json_dump_list (buffer, obj);
    /* object is of type unicode */
    else if (PyDict_Check (obj))
        return _json_dump_dict (buffer, obj);

    PyErr_Format (PyExc_TypeError, "can't dump object of type '%s'", obj->ob_type->tp_name);
    return NULL;
}

/*
 * 
 */
PyObject *
terane_json_dump (PyObject *obj)
{
    _json_buffer *buffer;
    PyObject *py_unicode;

    buffer = _json_buffer_new ();
    if (_json_dump_object (buffer, obj) == NULL) {
        _json_buffer_free (buffer);
        return NULL;
    }
    py_unicode = PyUnicode_DecodeUTF8 (buffer->buffer, buffer->length, NULL);
    _json_buffer_free (buffer);
    return py_unicode;
}
