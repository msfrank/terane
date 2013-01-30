/*
 * Copyright 2013 Michael Frank <msfrank@syntaxjockey.com>
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

struct flagdef {
    char *name;
    int value;
};

static int
_parse_flags (PyObject *kwds, struct flagdef *flagdefs)
{
    PyObject *key, *value;
    char *name;
    Py_ssize_t pos = 0, len;
    int i, dbflags = 0;

    assert (flagdefs != NULL);

    if (kwds == NULL)
        return dbflags;
    while (PyDict_Next (kwds, &pos, &key, &value)) {
        if (!PyString_Check (key))
            continue;
        if (PyString_AsStringAndSize (key, &name, &len) < 0)
            return -1;
        for (i = 0; flagdefs[i].name != NULL; i++) {
            if (!strncmp (flagdefs[i].name, name, len) && PyObject_IsTrue (value)) {
                dbflags |= flagdefs[i].value;
                break;
            }
        }
    }
    return dbflags;
}
    
int
_terane_parse_env_txn_begin_flags (PyObject *kwds)
{
    struct flagdef flagdefs[] = {
        { "READ_COMMITTED", DB_READ_COMMITTED },
        { "READ_UNCOMMITTED", DB_READ_UNCOMMITTED },
        { "TXN_NOSYNC", DB_TXN_NOSYNC },
        { "TXN_NOWAIT", DB_TXN_NOWAIT },
        { "TXN_SNAPSHOT", DB_TXN_SNAPSHOT }, 
        { "TXN_WRITE_NOSYNC", DB_TXN_WRITE_NOSYNC },
        { NULL, 0 }
    };
    return _parse_flags (kwds, flagdefs);
}    

int
_terane_parse_db_get_flags (PyObject *kwds)
{
    struct flagdef flagdefs[] = {
        { "READ_COMMITTED", DB_READ_COMMITTED },
        { "READ_UNCOMMITTED", DB_READ_UNCOMMITTED },
        { "RMW", DB_RMW },
        { NULL, 0 }
    };
    return _parse_flags (kwds, flagdefs);
}

int
_terane_parse_db_put_flags (PyObject *kwds)
{
    struct flagdef flagdefs[] = {
        { "NOOVERWRITE", DB_NOOVERWRITE },
        { NULL, 0 }
    };
    return _parse_flags (kwds, flagdefs);
}

int
_terane_parse_db_del_flags (PyObject *kwds)
{
    struct flagdef flagdefs[] = {
        { NULL, 0 }
    };
    return _parse_flags (kwds, flagdefs);
}

int
_terane_parse_db_exists_flags (PyObject *kwds)
{
    struct flagdef flagdefs[] = {
        { "READ_COMMITTED", DB_READ_COMMITTED },
        { "READ_UNCOMMITTED", DB_READ_UNCOMMITTED },
        { "RMW", DB_RMW },
        { NULL, 0 }
    };
    return _parse_flags (kwds, flagdefs);
}

int
_terane_parse_db_cursor_flags (PyObject *kwds)
{
    struct flagdef flagdefs[] = {
        { "READ_COMMITTED", DB_READ_COMMITTED },
        { "READ_UNCOMMITTED", DB_READ_UNCOMMITTED },
        { "TXN_SNAPSHOT", DB_TXN_SNAPSHOT }, 
        { NULL, 0 }
    };
    return _parse_flags (kwds, flagdefs);
}
