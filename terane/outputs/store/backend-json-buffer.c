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
    char *data;         /* allocated buffer */
    Py_ssize_t len;     /* length of the string in the buffer, not including the null terminator */
} terane_buffer;

/*
 * terane_buffer_new: allocate a new buffer.
 */
static terane_buffer *
terane_buffer_new (void)
{
    terane_buffer *buffer;

    buffer = PyMem_Malloc (sizeof (terane_buffer));
    if (buffer == NULL)
        return (terane_buffer *) PyErr_NoMemory ();
    memset (buffer, 0, sizeof (terane_buffer));
    return buffer;
}

/*
 * terane_buffer_free: free an allocated buffer.
 */
void
terane_buffer_free (terane_buffer *buffer, int freedata)
{
    assert (buffer != NULL);

    if (buffer->data && freedata)
        PyMem_Free (buffer->data);
    PyMem_Free (buffer);
}

/*
 * terane_buffer_append_c_str: append a C string to the specified buffer.
 */
static terane_buffer *
terane_buffer_append_c_str (terane_buffer *buffer, const char *c_str, Py_ssize_t len)
{
    char *data;

    assert (buffer != NULL);
    assert (c_str != NULL);
    assert (len >= 0);
    data = PyMem_Realloc (buffer->data, buffer->len + len + 1);
    if (data == NULL)
        return (terane_buffer *) PyErr_NoMemory ();
    buffer->data = data;
    strncpy (&buffer->data[buffer->len], c_str, len);
    buffer->len = buffer->len + len;
    buffer->data[buffer->len] = '\0';
    return buffer;
}

/*
 * terane_buffer_append_py_str: append a python str to the specified buffer.
 */
static terane_buffer *
terane_buffer_append_py_str (terane_buffer *buffer, PyObject *py_str)
{
    char *c_str = NULL;
    Py_ssize_t len = 0;

    assert (buffer != NULL);
    assert (py_str != NULL);
    if (PyString_AsStringAndSize (py_str, &c_str, &len) < 0)
        return NULL;
    return terane_buffer_append_c_str (buffer, c_str, len);
}

/*
 * terane_buffer_append_py_unicode: append a python unicode string to the specified buffer.
 */
static terane_buffer *
terane_buffer_append_py_unicode (terane_buffer *buffer, PyObject *py_unicode)
{
    PyObject *py_str = NULL;

    assert (buffer != NULL);
    assert (py_unicode != NULL);

    py_str = PyUnicode_AsUTF8String (py_unicode);
    if (py_str == NULL)
        return NULL;
    buffer = terane_buffer_append_py_str (buffer, py_str);
    Py_DECREF (py_str);
    return buffer;
}

/*
 * terane_buffer_append_utf8_char: append a terane_utf8_char to the specified buffer.
 */
static terane_buffer *
terane_buffer_append_utf8_char (terane_buffer *buffer, terane_utf8_char *ch)
{
    Py_ssize_t len;

    assert (buffer != NULL);
    assert (ch != NULL);

    len = terane_utf8_char_is_valid (ch);
    if (len == 0)
        return NULL;
    return terane_buffer_append_c_str (buffer, (const char *) ch, len);
}
