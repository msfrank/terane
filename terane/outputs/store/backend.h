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

#ifndef TERANE_OUTPUTS_STORE_BACKEND_H
#define TERANE_OUTPUTS_STORE_BACKEND_H

#include <Python.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <endian.h>
#include <db.h>
#include <pthread.h>
#include <assert.h>

/*
 * class object declarations
 */
typedef struct _terane_Env {
    PyObject_HEAD
    DB_ENV *env;
    pthread_t checkpoint_thread;
} terane_Env;

typedef struct _terane_Index {
    PyObject_HEAD
    terane_Env *env;
    PyObject *name;
    DB *metadata;
    DB *schema;
    DB *segments;
    unsigned long nfields;
} terane_Index;

typedef struct {
    int type;
    union {
        uint32_t u32;
        int32_t i32;
        uint64_t u64;
        int64_t i64;
        double f64;
        struct {
            char *bytes;
            Py_ssize_t size;
        } raw;
    } data;
} terane_value;

typedef union {
    uint8_t u8;
    int8_t i8;
    uint16_t u16;
    int16_t i16;
    uint32_t u32;
    int32_t i32;
    uint64_t u64;
    int64_t i64;
    float f32;
    double f64;
    struct {
        char *bytes;
        uint32_t size;
    } raw;
} terane_conv;

typedef struct {
    terane_value **values;
    Py_ssize_t size;
} terane_iterkey;

typedef struct _terane_Iter {
    PyObject_HEAD
    PyObject *parent;
    DBC *cursor;
    int initialized;
    int itype;
    terane_iterkey *start;
    terane_iterkey *end;
    terane_iterkey *prefix;
    DBT range;
    int reverse;
    PyObject *(*next)(struct _terane_Iter *, DBT *, DBT *);
    PyObject *(*skip)(struct _terane_Iter *, PyObject *);
} terane_Iter;

typedef PyObject *(*terane_Iter_next_cb)(terane_Iter *, DBT *, DBT *);
typedef PyObject *(*terane_Iter_skip_cb)(terane_Iter *, PyObject *);

typedef struct _terane_Iter_ops {
    terane_Iter_next_cb next;
    terane_Iter_skip_cb skip;
} terane_Iter_ops;

typedef struct _terane_Txn {
    PyObject_HEAD
    DB_TXN *txn;                    /* DB_TXN handle */
    terane_Env *env;                /* reference to the object holding the DB_ENV handle */
    struct _terane_Txn *children;   /* pointer to the first child Txn, or NULL */
    struct _terane_Txn *next;       /* pointer to the next child of the parent Txn, or NULL */
} terane_Txn;

typedef struct _terane_Segment {
    PyObject_HEAD
    terane_Index *index;    /* reference to the table of contents */
    char *name;             /* name of the segment file */
    DB *metadata;           /* DB handle to the segment metadata */
    DB *events;             /* DB handle to the segment events */
    DB *postings;           /* DB handle to the segment postings */
    DB *fields;             /* DB handle to the segment fields */
    DB *terms;              /* DB handle to the segment terms */
    int deleted;            /* non-zero if the segment is scheduled to be deleted */
} terane_Segment;


/*
 * class method definitions
 */

/* Env methods */
PyObject * terane_Env_close (terane_Env *self);

/* Index methods */
PyObject * terane_Index_get_meta (terane_Index *self, PyObject *args);
PyObject * terane_Index_set_meta (terane_Index *self, PyObject *args);

PyObject * terane_Index_get_field (terane_Index *self, PyObject *args);
PyObject * terane_Index_add_field (terane_Index *self, PyObject *args);
PyObject * terane_Index_iter_fields (terane_Index *self, PyObject *args);
PyObject * terane_Index_contains_field (terane_Index *self, PyObject *args);
PyObject * terane_Index_count_fields (terane_Index *self);

PyObject * terane_Index_new_segment (terane_Index *self, PyObject *args);
int        terane_Index_contains_segment (terane_Index *self, terane_Txn *txn, db_recno_t sid);
PyObject * terane_Index_iter_segments (terane_Index *self, PyObject *args);
PyObject * terane_Index_count_segments (terane_Index *self);
PyObject * terane_Index_delete_segment (terane_Index *self, PyObject *args);
PyObject * terane_Index_new_txn (terane_Index *self, PyObject *args, PyObject *kwds);
PyObject * terane_Index_close (terane_Index *self);

/* Segment methods */
PyObject * terane_Segment_get_meta (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_set_meta (terane_Segment *self, PyObject *args);

PyObject * terane_Segment_get_field (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_set_field (terane_Segment *self, PyObject *args);

PyObject * terane_Segment_new_event (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_get_event (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_set_event (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_delete_event (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_contains_event (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_estimate_events (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_iter_events (terane_Segment *self, PyObject *args);

PyObject * terane_Segment_get_term (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_set_term (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_iter_terms (terane_Segment *self, PyObject *args);

PyObject * terane_Segment_get_posting (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_set_posting (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_contains_posting (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_estimate_postings (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_iter_postings (terane_Segment *self, PyObject *args);

PyObject * terane_Segment_delete (terane_Segment *self);
PyObject * terane_Segment_close (terane_Segment *self);

/* Txn methods */
PyObject * terane_Txn_new (terane_Env *env, terane_Txn *parent, PyObject *args, PyObject *kwds);
PyObject * terane_Txn_new_txn (terane_Txn *self, PyObject *args, PyObject *kwds);
PyObject * terane_Txn_commit (terane_Txn *self);
PyObject * terane_Txn_abort (terane_Txn *self);

/* Iter methods */
PyObject * terane_Iter_new (PyObject *parent, DBC *cursor, terane_Iter_ops *ops, int reverse);
PyObject * terane_Iter_new_prefix (PyObject *parent, DBC *cursor, terane_Iter_ops *ops, PyObject *key, int reverse);
PyObject * terane_Iter_new_from (PyObject *parent, DBC *cursor, terane_Iter_ops *ops, PyObject *key, int reverse);
PyObject * terane_Iter_new_within (PyObject *parent, DBC *cursor, terane_Iter_ops *ops, PyObject *start, PyObject *end, int reverse);
PyObject * terane_Iter_skip (terane_Iter *self, PyObject *args);
PyObject * terane_Iter_close (terane_Iter *self);

/*
 * logging function declarations
 */
PyObject * terane_log_fd (PyObject *self);
void       terane_log_msg (int level, const char *logger, const char *fmt, ...);

/*
 * msgpack serialization declarations
 */
int             _terane_msgpack_dump (PyObject *obj, char **buf, uint32_t *len);
PyObject *      terane_msgpack_dump (PyObject *self, PyObject *args);
terane_value *  _terane_msgpack_make_value (PyObject *obj);
int             _terane_msgpack_load_value (char *buf, uint32_t len, char **pos, terane_value *val);
int             _terane_msgpack_load (char *buf, uint32_t len, PyObject **dest);
PyObject *      terane_msgpack_load (PyObject *self, PyObject *args);
int             _terane_msgpack_cmp_values (terane_value *v1, terane_value *v2);
int             _terane_msgpack_cmp (char *b1, uint32_t l1, char *b2, uint32_t l2, int *result);
int             _terane_msgpack_DB_compare (DB *db, const DBT *dbt1, const DBT *dbt2);
void            _terane_msgpack_free_value (terane_value *value);

/*
 * class type declarations
 */
extern PyTypeObject terane_IterType;
extern PyTypeObject terane_EnvType;
extern PyTypeObject terane_TxnType;
extern PyTypeObject terane_SegmentType;
extern PyTypeObject terane_IndexType;

/* 
 * exception type declarations
 */
extern PyObject *terane_Exc_Deadlock;
extern PyObject *terane_Exc_LockTimeout;
extern PyObject *terane_Exc_DocExists;
extern PyObject *terane_Exc_Error;


/*
 * msgpack type constants
 */
typedef enum {
    TERANE_MSGPACK_TYPE_UNKNOWN = 0,
    TERANE_MSGPACK_TYPE_NONE    = 1,
    TERANE_MSGPACK_TYPE_FALSE   = 2,
    TERANE_MSGPACK_TYPE_TRUE    = 3,
    TERANE_MSGPACK_TYPE_INT64   = 4,
    TERANE_MSGPACK_TYPE_INT32   = 5,
    TERANE_MSGPACK_TYPE_UINT32  = 6,
    TERANE_MSGPACK_TYPE_UINT64  = 7,
    TERANE_MSGPACK_TYPE_DOUBLE  = 8,
    TERANE_MSGPACK_TYPE_RAW     = 9,
    TERANE_MSGPACK_TYPE_LIST    = 10,
    TERANE_MSGPACK_TYPE_DICT    = 11
} terane_msgpack_type;

/*
 * iteration type constants
 */
#define TERANE_ITER_ALL         1
#define TERANE_ITER_PREFIX      2
#define TERANE_ITER_FROM        3
#define TERANE_ITER_WITHIN      4

/* 
 * logging levels.  these values correspond to the logging levels in
 * the 'logging' package in the python standard library.  for more
 * information see http://docs.python.org/library/logging.html#logging-levels.
 */
#define TERANE_LOG_FATAL            0
#define TERANE_LOG_ERROR            10
#define TERANE_LOG_WARNING          20
#define TERANE_LOG_INFO             30
#define TERANE_LOG_DEBUG            40
#define TERANE_LOG_TRACE            50

/* TERANE_OUTPUTS_STORE_BACKEND_H */
#endif
