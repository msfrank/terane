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
 * _json_load_str:
 */
static PyObject
_json_load_str (terane_utf8 *iter)
{
    terane_utf8_char ch;
    terane_buffer *buffer = NULL;
    unsigned char escape[5] = { 0, 0, 0, 0, 0 };
    int escape_count = 0;

    buffer = terane_buffer_new ();

    /* we know this succeeds because we've already peeked */
    terane_utf8_next (iter, &ch);
    while (1) {
        Py_ssize_t ret = terane_utf8_next (iter, &ch);
        /* we reached the end without finding the trailing quote */
        if (ret == 0) {
            PyErr_Format (PyExc_ValueError, "Unexpected end of input");
            goto error;
        }
        /* the input contains invalid UTF-8 data */
        else if (ret < 0) {
            PyErr_Format (PyExc_ValueError, "Invalid UTF-8 input");
            goto error;
        }
        /* if we are processing an escape code */
        if (escape_count > 0) {
            /* all escape chars must be valid ASCII */
            if (ch.o1 & 0x80) {
                PyErr_Format (PyExc_ValueError, "Invalid UTF-8 input");
                goto error;
            }
            /* figure out what kind of escape we are dealing with */
            if (escape_count == 1) {
                switch ((char) ch.o1) {
                    case 'u':
                        escape_count++;
                        break;
                    case 'b':
                        ch.o1 = (unsigned char) '\b';
                        terane_buffer_append_utf8_char (buffer, &ch);
                        escape_count = 0;
                        break;
                    case 'f':
                        ch.o1 = (unsigned char) '\f';
                        terane_buffer_append_utf8_char (buffer, &ch);
                        escape_count = 0;
                        break;
                    case 'n':
                        ch.o1 = (unsigned char) '\n';
                        terane_buffer_append_utf8_char (buffer, &ch);
                        escape_count = 0;
                        break;
                    case 'r':
                        ch.o1 = (unsigned char) '\r';
                        terane_buffer_append_utf8_char (buffer, &ch);
                        escape_count = 0;
                        break;
                    case 't':
                        ch.o1 = (unsigned char) '\t';
                        terane_buffer_append_utf8_char (buffer, &ch);
                        escape_count = 0;
                        break;
                    default:
                        terane_buffer_append_utf8_char (buffer, &ch);
                        escape_count = 0;
                        break;
                }
            }
            /* parse a unicode escape */
            else {
                char c = (char) ch.o1;
                /* escape char must be a valid hexadecimal char */
                if (!(c >= 0x30 && c <= 0x39) &&
                    !(c >= 0x41 && c <= 0x46) &&
                    !(c >= 0x61 && c <= 0x66)) {
                    PyErr_Format (PyExc_ValueError, "Invalid UTF-8 input");
                    goto error;
                }
                /* put the escape char in the temporary buffer */
                escape[escape_count - 2] = ch.o1;
                escape_count++;
                /* we have finished parsing the unicode escape */
                if (escape_count > 5) {
                    uint32_t value;
                    char *endptr = NULL;
                    /* stop processing escape characters */
                    escape_count = 0;
                    /* parse the escape sequence into an int */
                    value = (uint32_t) strtoul ((const char *) escape, &endptr, 16);
                    if (*endptr != '\0') {
                        PyErr_Format (PyExc_ValueError, "Invalid UTF-8 input");
                        goto error;
                    }
                    /* convert the int to network byte order */
                    value = htonl (value);
                    /* make sure value is valid UTF-8 */
                    if (!terane_utf8_char_is_valid ((terane_utf8_char *) value)) {
                        PyErr_Format (PyExc_ValueError, "Invalid UTF-8 input");
                        goto error;
                    }
                    /* add the value to the buffer */
                    if (terane_buffer_append_utf8_char (buffer, (terane_utf8_char *) &value) == NULL)
                        goto error;
                }
            }
        }
        else {
            /* if we find a backslash, start processing escape */
            if ((char) ch.o1 == '\\')
                escape_count = 1;
            /* if we find a double quote, then we are done */
            else if ((char) ch.o1 == '"') {
                PyObject *str = PyUnicode_DecodeUTF8 (buffer->data, buffer->len, "strict");
                terane_buffer_free (buffer, 1);
                return str;
            }
            /* otherwise add the char to the buffer */
            else {
                if (terane_buffer_append_utf8_char (buffer, &ch) == NULL)
                    goto error;
            }
        }
    }

error:
    if (buffer)
        terane_buffer_free (buffer, 1);
    return NULL;
}

/*
 * _json_load_number:
 */
static PyObject *
_json_load_number (terane_utf8 *iter)
{
    terane_utf8_char ch;
    terane_buffer *buffer;
    Py_ssize_t ret;

    buffer = terane_buffer_new ();

    /* we know this succeeds because we've already peeked */
    terane_utf8_next (iter, &ch);
    while (1) {
        Py_ssize_t ret = terane_utf8_peek (iter, &ch);
        /* the input contains invalid UTF-8 data */
        if (ret < 0) {
            PyErr_Format (PyExc_ValueError, "Invalid UTF-8 input");
            goto error;
        }
        if (ret == 0 || !terane_utf8_char_in ("-e.0123456789", &ch))
            break;
        /* consume the char and add it to the buffer
        terane_utf8_next (iter, &ch);
        if (terane_buffer_append_utf8_char (buffer, &ch) == NULL)
            goto error;
    }
error:
    if (buffer)
        terane_buffer_free (buffer, 1);
    return NULL;
}

/*
 * _json_load_dict:
 */
static PyObject *
_json_load_dict (terane_utf8 *iter)
{
    _utf8_char ch;

    if (_utf8_get (iter, &ch) < 0)
        return NULL;
    if (_utf8_char_in ("{", &ch) < 0)
        return NULL;
}

/*
 * _json_load_object:
 */
static PyObject *
_json_load_object (terane_utf8 *iter)
{
    terane_utf8_char ch;

    if (terane_utf8_chomp (iter) < 0)
        return NULL;
    if (terane_utf8_peek (iter, &ch) < 0)
        return NULL;
    if (terane_utf8_char_in ("{", ch))
        return _json_load_dict (iter);
    if (terane_utf8_char_in ("[", ch))
        return _json_load_list (iter);
    if (terane_utf8_char_in ("\"", ch))
        return _json_load_str (iter);
    if (terane_utf8_char_in ("0123456789-", ch))
        return _json_load_number (iter);
    if (terane_utf8_char_in ("tf", ch))
        return _json_load_bool (iter);
    if (terane_utf8_char_in ("n", ch))
        return _json_load_null (iter);
    return NULL;
}

/*
 * 
 */
PyObject *
terane_json_load (char *data, Py_ssize_t len)
{
    terane_utf8 iter;

    terane_utf8_init (&iter, (unsigned char *) data, len);
    return _json_load_object (&iter);
}
