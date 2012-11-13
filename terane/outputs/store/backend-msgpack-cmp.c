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
 * _terane_msgpack_cmp_values:
 */
int
_terane_msgpack_cmp_values (terane_value *v1, terane_value *v2)
{
    int ret;

    if (v1->type == v2->type) {
        switch (v1->type) {
            case TERANE_MSGPACK_TYPE_NONE:
            case TERANE_MSGPACK_TYPE_FALSE:
            case TERANE_MSGPACK_TYPE_TRUE:
                return 0;
            case TERANE_MSGPACK_TYPE_UINT32:
                if (v1->data.u32 < v2->data.u32)
                    return -1;
                if (v1->data.u32 > v2->data.u32)
                    return 1;
                return 0;
            case TERANE_MSGPACK_TYPE_INT32:
                if (v1->data.i32 < v2->data.i32)
                    return -1;
                if (v1->data.i32 > v2->data.i32)
                    return 1;
                return 0;
            case TERANE_MSGPACK_TYPE_UINT64:
                if (v1->data.u64 < v2->data.u64)
                    return -1;
                if (v1->data.u64 > v2->data.u64)
                    return 1;
                return 0;
            case TERANE_MSGPACK_TYPE_INT64:
                if (v1->data.i64 < v2->data.i64)
                    return -1;
                if (v1->data.i64 > v2->data.i64)
                    return 1;
                return 0;
            case TERANE_MSGPACK_TYPE_DOUBLE:
                if (v1->data.f64 < v2->data.f64)
                    return -1;
                if (v1->data.f64 > v2->data.f64)
                    return 1;
                return 0;
            case TERANE_MSGPACK_TYPE_RAW:
                ret = v1->data.raw.size < v2->data.raw.size ?
                  memcmp (v1->data.raw.bytes, v2->data.raw.bytes, v1->data.raw.size) :
                  memcmp (v1->data.raw.bytes, v2->data.raw.bytes, v2->data.raw.size);
                if (ret != 0)
                    return ret;
                if (v1->data.raw.size == v2->data.raw.size)
                    return 0;
                ret = v1->data.raw.size < v2->data.raw.size ? -1 : 1;
                return ret;
            default:
                return 0;
        }
    }
    if (v1->type < v2->type)
        return -1;
    return 1;
}

/*
 * _terane_msgpack_cmp:
 */
int
_terane_msgpack_cmp (char *b1, uint32_t l1, char *b2, uint32_t l2, int *result)
{
    char *pos1 = NULL, *pos2 = NULL;
    terane_value val1, val2;
    int ret1, ret2;

    assert (b1 && l1 > 0);
    assert (b2 && l2 > 0);
    assert (result != NULL);

    *result = 0;

    while (1) {
        /* if either load returns error, then pass that along */
        if ((ret1 = _terane_msgpack_load_value (b1, l1, &pos1, &val1)) < 0)
            return -1;
        if ((ret2 = _terane_msgpack_load_value (b2, l2, &pos2, &val2)) < 0)
            return -1;
        /* if both lists have no more items, then they compare equal */
        if (ret1 == 0 && ret2 == 0)
            return 0;
        /* 
         * if either unpacker returns 0, then we don't need to compare the
         * actual objects.  otherwise, compare the two objects themselves.
         * if the result is zero then compare the next set of objects,
         * otherwise, break the loop.
         */
        if (ret1 == 0)
            *result = -1;
        else if (ret2 == 0)
            *result = 1;
        else
            *result = _terane_msgpack_cmp_values (&val1, &val2);
        /* if result is not 0, then we are done */
        if (*result != 0)
            break;
    }
    return 0;
}

/*
 * _terane_msgpack_DB_compare: 
 */
int
_terane_msgpack_DB_compare (DB *db, const DBT *dbt1, const DBT *dbt2)
{
    int result = 0;

    if (_terane_msgpack_cmp (dbt1->data, dbt1->size,
      dbt2->data, dbt2->size, &result) < 0) {
        terane_log_msg (TERANE_LOG_ERROR, "terane.outputs.store.backend",
            "_terane_msgpack_DB_compare: comparison failed");
    }
    return result;
}
