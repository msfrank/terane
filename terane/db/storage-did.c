/*
 * Copyright 2010,2011 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Diggle.
 *
 * Diggle is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * Diggle is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Diggle.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "storage.h"

/*
 * DID_num_to_string: convert a document id integer into a string.
 */
int
DID_num_to_string (diggle_DID_num did_num, diggle_DID_string did_string)
{
    assert (did_string != NULL);
    if (snprintf (did_string, DIGGLE_DID_STRING_LEN, "%016llx", did_num) < 16)
        return -1;
    return 0;
}

/*
 * DocID_string_to_num: convert a document id string into an integer.
 */
int
DID_string_to_num (diggle_DID_string did_string, diggle_DID_num *did_num)
{
    char *endptr = NULL;

    assert (did_string != NULL);
    assert (did_num != NULL);
    *did_num = strtoull ((const char *) did_string, &endptr, 16);
    if (did_string[0] != '\0' && *endptr == '\0')
        return 0;
    return -1;
}
