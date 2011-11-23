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
 * terane_DID_num_to_string: convert a document id integer into a string.
 */
int
terane_DID_num_to_string (terane_DID_num did_num, terane_DID_string did_string)
{
    assert (did_string != NULL);
    if (snprintf (did_string, TERANE_DID_STRING_LEN, "%016llx", did_num) < 16)
        return -1;
    return 0;
}

/*
 * terane_DID_string_to_num: convert a document id string into an integer.
 */
int
terane_DID_string_to_num (terane_DID_string did_string, terane_DID_num *did_num)
{
    char *endptr = NULL;

    assert (did_string != NULL);
    assert (did_num != NULL);
    *did_num = strtoull ((const char *) did_string, &endptr, 16);
    if (did_string[0] != '\0' && *endptr == '\0')
        return 0;
    return -1;
}
