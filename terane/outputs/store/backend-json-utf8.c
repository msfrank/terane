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
    unsigned char o1;
    unsigned char o2;
    unsigned char o3;
    unsigned char o4;
} terane_utf8_char;

typedef struct {
    unsigned char *data;
    Py_ssize_t len;
    Py_ssize_t curr;
} terane_utf8;

void
terane_utf8_init (terane_utf8 *utf8, unsigned char *data, Py_ssize_t len)
{
    assert (utf8 != NULL);
    assert (data != NULL);
    assert (len >= 0);

    memset (utf8, 0, sizeof (_utf8));

    utf8->data = data;
    utf->len = len;
    utf8->curr = 0;
}

static int
_utf8_is_valid_leading_byte (unsigned char uchar)
{
    if ((uchar & 0xC0) == 0x80 || uchar == 0xC0 || uchar == 0xC1 || uchar > 0xF4)
        return 0;
    return 1;
}

static int
_utf8_is_valid_continuation_byte (unsigned char uchar)
{
    if ((uchar & 0xC0) == 0x80)
        return 1;
    return 0;
}

/*
 * terane_utf8_char_is_valid: checks whether the specified utf8 char is valid.
 *  if not, then returns 0, otherwise, returns a positive value representing the
 *  number of bytes needed to represent the UTF-8 char.
 */
Py_ssize_t
terane_utf8_char_is_valid (terane_utf8_char *ch)
{
    assert (ch != NULL);

    if (!_utf8_is_valid_leading_byte (ch.o1))
        return 0;
    if (ch.o1 & 0xC0 == 0xC0) {
        if (!_utf8_is_valid_continuation_byte (ch.o2))
            return 0;
        if (ch.o1 & 0xE0 == 0xE0) {
            if (!_utf8_is_valid_continuation_byte (ch.o3))
                return 0;
            if (ch.o1 & 0xF0 == 0xF0) {
                if (!_utf8_is_valid_continuation_byte (ch.o3))
                    return 0;
                return 4;
            }
            else
                return 3;
        }
        else
            return 2;
    }
    else
        return 1;
}

/*
 * terane_utf8_char_in: checks whether any of the specified ascii chars in delim
 *  match ch.  if not, then returns 0, otherwise, returns a positive value
 *  representing the number of bytes needed to represent the UTF-8 char.  on
 *  error, returns a negative value.
 */
Py_ssize_t
terane_utf8_char_in (const char *delim, terane_utf_char *ch)
{
    assert (ch != NULL);
    assert (delim != NULL);

    if (terane_utf8_char_is_valid (ch) != 1)
        return -1;
    while (*delim != '\0') {
        if (*delim == (char) ch.o1)
            return 1;
        delim++;
    }
    return 0;
}

/*
 * terane_utf8_peek: looks ahead to the next UTF-8 char, but does not consume it.
 */
Py_ssize_t
terane_utf8_peek (terane_utf8 *utf8, terane_utf8_char *ch)
{
    Py_ssize_t curr, nbytes = 0;

    assert (utf8 != NULL);
    assert (utf8->curr <= utf8->len);
    assert (ch != NULL);

    curr = utf8->curr;
    if (curr == utf8->len)
        return 0;
    ch.o1 = utf8->data[curr++];
    nbytes++;
    if (!_utf8_is_valid_leading_byte (ch.o1))
        return -nbytes;
    if (ch.o1 & 0xC0 == 0xC0) {
        if (curr == utf8->len)
            return -octets;
        ch.o2 = utf->data[curr++];
        octets++;
        if (!_utf8_is_valid_continuation_byte (ch.o2))
            return -nbytes;
        if (ch.o1 & 0xE0 == 0xE0) {
            if (curr == utf8->len)
                return -nbytes;
            ch.o3 = utf->data[curr++];
            nbytes++;
            if (!_utf8_is_valid_continuation_byte (ch.o3))
                return -nbytes;
            if (ch.o1 & 0xF0 == 0xF0) {
                if (curr == utf8->len)
                    return -nbytes;
                ch.o4 = utf->data[curr];
                nbytes++;
                if (!_utf8_is_valid_continuation_byte (ch.o3))
                    return -nbytes;
            }
        }
    }
    return nbytes;
}

Py_ssize_t
terane_utf8_next (terane_utf8 *utf8, terane_utf8_char *ch)
{
    Py_ssize_t ret;

    assert (utf8 != NULL);
    assert (ch != NULL);
    
    ret = terane_utf8_peek (utf8, ch);
    if (ret > 0)
        utf8->curr += ret;
    return ret;
}

Py_ssize_t
terane_utf8_chomp (terane_utf8 *utf8)
{
    Py_ssize_t ret;

    assert (utf8 != NULL);

    while (1) {
        ret = terane_utf8_peek (utf8, ch);
        if (ret < 0)
            return -1;
        else if (ret == 0)
            return 0;
        else if (!(ch.o1 & 0x80) && isspace ((int) ch.o1))
            utf8->curr += ret;
        else
            break;
    }
    return 1;
}
