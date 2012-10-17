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
#include <db.h>
#include <pthread.h>
#include <assert.h>
#include <msgpack.h>

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

typedef struct _terane_Iter {
    PyObject_HEAD
    PyObject *parent;
    DBC *cursor;
    int initialized;
    int itype;
    DBT start_key;
    DBT end_key;
    int reverse;
    PyObject *(*next)(struct _terane_Iter *, DBT *, DBT *);
    DBT *(*skip)(struct _terane_Iter *, PyObject *);
} terane_Iter;

typedef PyObject *(*terane_Iter_next_cb)(terane_Iter *, DBT *, DBT *);
typedef DBT *(*terane_Iter_skip_cb)(terane_Iter *, PyObject *);

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
    DB *documents;          /* DB handle to the segment documents */
    DB *postings;           /* DB handle to the segment postings */
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
PyObject * terane_Index_remove_field (terane_Index *self, PyObject *args);
PyObject * terane_Index_contains_field (terane_Index *self, PyObject *args);
PyObject * terane_Index_list_fields (terane_Index *self, PyObject *args);
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
PyObject * terane_Segment_get_field_meta (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_set_field_meta (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_new_doc (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_get_doc (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_set_doc (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_delete_doc (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_contains_doc (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_estimate_docs (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_iter_docs_within (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_get_term (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_set_term (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_contains_term (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_estimate_term_postings (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_iter_terms (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_iter_terms_within (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_get_term_meta (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_set_term_meta (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_iter_terms_meta (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_iter_terms_meta_range (terane_Segment *self, PyObject *args);
PyObject * terane_Segment_delete (terane_Segment *self);
PyObject * terane_Segment_close (terane_Segment *self);

/* Txn methods */
PyObject * terane_Txn_new (terane_Env *env, terane_Txn *parent, PyObject *args, PyObject *kwds);
PyObject * terane_Txn_new_txn (terane_Txn *self, PyObject *args, PyObject *kwds);
PyObject * terane_Txn_commit (terane_Txn *self);
PyObject * terane_Txn_abort (terane_Txn *self);

/* Iter methods */
PyObject * terane_Iter_new (PyObject *parent, DBC *cursor, terane_Iter_ops *ops, int reverse);
PyObject * terane_Iter_new_range (PyObject *parent, DBC *cursor, terane_Iter_ops *ops, void *key, size_t len, int reverse);
PyObject * terane_Iter_new_from (PyObject *parent, DBC *cursor, terane_Iter_ops *ops, void *key, size_t len, int reverse);
PyObject * terane_Iter_new_within (PyObject *parent, DBC *cursor, terane_Iter_ops *ops, DBT *start, DBT *end, int reverse);
PyObject * terane_Iter_skip (terane_Iter *self, PyObject *args);
PyObject * terane_Iter_close (terane_Iter *self);

/*
 * logging function declarations
 */
void       terane_log_msg (int level, const char *logger, const char *fmt, ...);
PyObject * terane_Module_log_fd (PyObject *self);

/*
 * msgpack serialization declarations
 */
int        _terane_msgpack_dump (PyObject *obj, char **buf, uint32_t *len);
PyObject * terane_msgpack_dump (PyObject *self, PyObject *args);
int        _terane_msgpack_load (const char *buf, uint32_t len, PyObject **dest);
PyObject * terane_msgpack_load (PyObject *self, PyObject *args);

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
 * iteration type constants
 */
#define TERANE_ITER_ALL         1
#define TERANE_ITER_RANGE       2
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
