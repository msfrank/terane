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

#ifndef TERANE_STORAGE_H
#define TERANE_STORAGE_H

#include <Python.h>
#include <db.h>
#include <pthread.h>
#include <assert.h>


/* Env object declaration */
typedef struct _terane_Env {
    PyObject_HEAD
    DB_ENV *env;
    PyObject *logger;
    pthread_t checkpoint_thread;
} terane_Env;

typedef struct _terane_TOC {
    PyObject_HEAD
    terane_Env *env;
    PyObject *name;
    DB *metadata;
    DB *schema;
    DB *segments;
    unsigned long nsegments;
    unsigned long nfields;
} terane_TOC;

/* Iter object declaration */
typedef struct _terane_Iter {
    PyObject_HEAD
    PyObject *parent;
    DBC *cursor;
    int initialized;
    int itype;
    void *key;
    size_t len;
    PyObject *(*next)(struct _terane_Iter *, DBT *, DBT *);
    DBT *(*skip)(struct _terane_Iter *, PyObject *);
} terane_Iter;

/* iteration callback declarations */ 
typedef PyObject *(*terane_Iter_next_cb)(terane_Iter *, DBT *, DBT *);
typedef DBT *(*terane_Iter_skip_cb)(terane_Iter *, PyObject *);

typedef struct _terane_Iter_ops {
    terane_Iter_next_cb next;
    terane_Iter_skip_cb skip;
} terane_Iter_ops;

/* Txn object declaration */
typedef struct _terane_Txn {
    PyObject_HEAD
    DB_TXN *txn;
    terane_Env *env;
    struct _terane_Txn *children;   /* pointer to the first child Txn, or NULL */
    struct _terane_Txn *next;       /* pointer to the next child of the parent Txn, or NULL */
} terane_Txn;

/* Field type declaration */
typedef struct _terane_Field {
    PyObject *name;
    DB *field;
} terane_Field;

/* Segment type declaration */
typedef struct _terane_Segment {
    PyObject_HEAD
    terane_TOC *toc;
    terane_Env *env;
    char *name;
    DB *documents;
    unsigned long ndocuments;
    terane_Field **fields;
    unsigned long nfields;
    int deleted;
} terane_Segment;

typedef unsigned PY_LONG_LONG terane_DID_num;
typedef char terane_DID_string[17];


/*
 * class method definitions
 */
PyObject *terane_Env_new (PyTypeObject *type, PyObject *args, PyObject *kwds);
PyObject *terane_Env_close (terane_Env *self, PyObject *args);
void Env_log (terane_Env *env, int level, const char *fmt, ...);

PyObject *terane_Txn_new (PyTypeObject *type, PyObject *args, PyObject *kwds);
PyObject *terane_Txn_commit (terane_Txn *self);
PyObject *terane_Txn_abort (terane_Txn *self);

terane_DID_num TOC_new_DID (terane_TOC *toc);
PyObject *terane_TOC_get_metadata (terane_TOC *self, PyObject *args);
PyObject *terane_TOC_set_metadata (terane_TOC *self, PyObject *args);
PyObject *terane_TOC_get_field (terane_TOC *self, PyObject *args);
PyObject *terane_TOC_add_field (terane_TOC *self, PyObject *args);
PyObject *terane_TOC_remove_field (terane_TOC *self, PyObject *args);
int TOC_contains_field (terane_TOC *toc, DB_TXN *txn, PyObject *fieldname);
PyObject *terane_TOC_contains_field (terane_TOC *self, PyObject *args);
PyObject *terane_TOC_list_fields (terane_TOC *self, PyObject *args);
PyObject *terane_TOC_count_fields (terane_TOC *self, PyObject *args);
PyObject *terane_TOC_new_segment (terane_TOC *toc, PyObject *args);
int TOC_contains_segment (terane_TOC *toc, terane_Txn *txn, db_recno_t segment_id);
PyObject *terane_TOC_iter_segments (terane_TOC *toc, PyObject *args);
PyObject *terane_TOC_count_segments (terane_TOC *toc, PyObject *args);
PyObject *terane_TOC_delete_segment (terane_TOC *toc, PyObject *args);
PyObject *terane_TOC_close (terane_TOC *toc);

PyObject *terane_Segment_new (PyTypeObject *type, PyObject *args, PyObject *kwds);
PyObject *terane_Segment_get_field_meta (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_set_field_meta (terane_Segment *self, PyObject *args);
DB *Segment_get_field_DB (terane_Segment *store, terane_Txn *txn, PyObject *fieldname);
PyObject *terane_Segment_delete (terane_Segment *self);
PyObject *terane_Segment_close (terane_Segment *self);

PyObject *terane_Segment_new_doc (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_get_doc (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_set_doc (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_delete_doc (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_contains_doc (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_iter_docs (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_count_docs (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_first_doc (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_last_doc (terane_Segment *self, PyObject *args);

PyObject *terane_Segment_get_word (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_set_word (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_contains_word (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_iter_words (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_get_word_meta (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_set_word_meta (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_iter_words_meta (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_iter_words_meta_from (terane_Segment *self, PyObject *args);
PyObject *terane_Segment_iter_words_meta_range (terane_Segment *self, PyObject *args);

PyObject *Iter_new (PyObject *parent, DBC *cursor, terane_Iter_ops *ops);
PyObject *Iter_new_range (PyObject *parent, DBC *cursor, terane_Iter_ops *ops, void *key, size_t len);
PyObject *Iter_new_from (PyObject *parent, DBC *cursor, terane_Iter_ops *ops, void *key, size_t len);
PyObject *terane_Iter_skip (terane_Iter *self, PyObject *args);
PyObject *terane_Iter_reset (terane_Iter *self, PyObject *args);
PyObject *terane_Iter_close (terane_Iter *self, PyObject *args);

int DID_num_to_string (terane_DID_num doc_num, terane_DID_string doc_str);
int DID_string_to_num (terane_DID_string doc_str, terane_DID_num *doc_num);


/*
 * module function declarations
 */
PyObject *terane__get_logfd (PyObject *self, PyObject *args);
void log_msg (int level, const char *logger, const char *fmt, ...);

/*
 * class type declarations
 */

/* Iter type declaration */
extern PyTypeObject terane_IterType;
 
/* Env type declaration */
extern PyTypeObject terane_EnvType;

/* Txn type declaration */
extern PyTypeObject terane_TxnType;

/* Segment type declaration */
extern PyTypeObject terane_SegmentType;

/* TOC type declaration */
extern PyTypeObject terane_TOCType;


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

/* logging levels */
/*
 * these values correspond to the logging levels in the 'logging' package
 * in the python standard library.  for more information see
 * http://docs.python.org/library/logging.html#logging-levels.
 */
#define TERANE_LOG_CRITICAL         50
#define TERANE_LOG_ERROR            40
#define TERANE_LOG_WARNING          30
#define TERANE_LOG_INFO             20
#define TERANE_LOG_DEBUG            10
#define TERANE_LOG_NOTSET           0

/* the size of a terane_DID_string buffer, including the trailing '\0' */
#define TERANE_DID_STRING_LEN       17

#endif
