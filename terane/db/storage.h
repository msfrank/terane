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

#ifndef DIGGLE_STORAGE_H
#define DIGGLE_STORAGE_H

#include <Python.h>
#include <db.h>
#include <pthread.h>
#include <assert.h>


/* Env object declaration */
typedef struct _diggle_Env {
    PyObject_HEAD
    DB_ENV *env;
    PyObject *logger;
    pthread_t cp_thread;
} diggle_Env;

typedef struct _diggle_TOC {
    PyObject_HEAD
    diggle_Env *env;
    PyObject *name;
    DB *metadata;
    DB *schema;
    DB *segments;
} diggle_TOC;

/* Iter object declaration */
typedef struct _diggle_Iter {
    PyObject_HEAD
    DBC *cursor;
    int initialized;
    int itype;
    void *key;
    size_t len;
    PyObject *(*next)(struct _diggle_Iter *, DBT *, DBT *);
    DBT *(*skip)(struct _diggle_Iter *, PyObject *);
} diggle_Iter;

/* iteration callback declarations */ 
typedef PyObject *(*diggle_Iter_next_cb)(diggle_Iter *, DBT *, DBT *);
typedef DBT *(*diggle_Iter_skip_cb)(diggle_Iter *, PyObject *);

typedef struct _diggle_Iter_ops {
    diggle_Iter_next_cb next;
    diggle_Iter_skip_cb skip;
} diggle_Iter_ops;

/* Txn object declaration */
typedef struct _diggle_Txn {
    PyObject_HEAD
    DB_TXN *txn;
    diggle_Env *env;
    struct _diggle_Txn *children;   /* pointer to the first child Txn, or NULL */
    struct _diggle_Txn *next;       /* pointer to the next child of the parent Txn, or NULL */
} diggle_Txn;

/* Field type declaration */
typedef struct _diggle_Field {
    PyObject *name;
    DB *field;
} diggle_Field;

/* Segment type declaration */
typedef struct _diggle_Segment {
    PyObject_HEAD
    diggle_TOC *toc;
    diggle_Env *env;
    char *name;
    DB *documents;
    unsigned long ndocuments;
    diggle_Field **fields;
    int nfields;
} diggle_Segment;

typedef unsigned PY_LONG_LONG diggle_DID_num;
typedef char diggle_DID_string[17];


/*
 * class method definitions
 */
PyObject *diggle_Env_new (PyTypeObject *type, PyObject *args, PyObject *kwds);
PyObject *diggle_Env_close (diggle_Env *self, PyObject *args);
void Env_log (diggle_Env *env, int level, const char *fmt, ...);

PyObject *diggle_Txn_new (PyTypeObject *type, PyObject *args, PyObject *kwds);
PyObject *diggle_Txn_commit (diggle_Txn *self);
PyObject *diggle_Txn_abort (diggle_Txn *self);

diggle_DID_num TOC_new_DID (diggle_TOC *toc);
PyObject *diggle_TOC_get_metadata (diggle_TOC *self, PyObject *args);
PyObject *diggle_TOC_set_metadata (diggle_TOC *self, PyObject *args);
PyObject *diggle_TOC_get_field (diggle_TOC *self, PyObject *args);
PyObject *diggle_TOC_add_field (diggle_TOC *self, PyObject *args);
PyObject *diggle_TOC_remove_field (diggle_TOC *self, PyObject *args);
int TOC_contains_field (diggle_TOC *toc, DB_TXN *txn, PyObject *fieldname);
PyObject *diggle_TOC_contains_field (diggle_TOC *self, PyObject *args);
PyObject *diggle_TOC_list_fields (diggle_TOC *self, PyObject *args);
PyObject *diggle_TOC_count_fields (diggle_TOC *self, PyObject *args);
PyObject *diggle_TOC_new_segment (diggle_TOC *toc, PyObject *args);
int TOC_contains_segment (diggle_TOC *toc, diggle_Txn *txn, db_recno_t segment_id);
PyObject *diggle_TOC_iter_segments (diggle_TOC *toc, PyObject *args);
PyObject *diggle_TOC_count_segments (diggle_TOC *toc, PyObject *args);

PyObject *diggle_Segment_new (PyTypeObject *type, PyObject *args, PyObject *kwds);
PyObject *diggle_Segment_get_field_meta (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_set_field_meta (diggle_Segment *self, PyObject *args);
DB *Segment_get_field_DB (diggle_Segment *store, diggle_Txn *txn, PyObject *fieldname);

PyObject *diggle_Segment_new_doc (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_get_doc (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_set_doc (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_delete_doc (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_contains_doc (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_iter_docs (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_count_docs (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_first_doc (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_last_doc (diggle_Segment *self, PyObject *args);

PyObject *diggle_Segment_get_word (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_set_word (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_contains_word (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_iter_words (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_get_word_meta (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_set_word_meta (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_iter_words_meta (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_iter_words_meta_from (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_iter_words_meta_range (diggle_Segment *self, PyObject *args);
PyObject *diggle_Segment_close (diggle_Segment *self);

PyObject *Iter_new (DBC *cursor, diggle_Iter_ops *ops);
PyObject *Iter_new_range (DBC *cursor, diggle_Iter_ops *ops, void *key, size_t len);
PyObject *Iter_new_from (DBC *cursor, diggle_Iter_ops *ops, void *key, size_t len);
PyObject *diggle_Iter_skip (diggle_Iter *self, PyObject *args);
PyObject *diggle_Iter_reset (diggle_Iter *self, PyObject *args);
PyObject *diggle_Iter_close (diggle_Iter *self, PyObject *args);

int DID_num_to_string (diggle_DID_num doc_num, diggle_DID_string doc_str);
int DID_string_to_num (diggle_DID_string doc_str, diggle_DID_num *doc_num);


/*
 * module function declarations
 */
PyObject *diggle__get_logfd (PyObject *self, PyObject *args);
void log_msg (int level, const char *logger, const char *fmt, ...);

/*
 * class type declarations
 */

/* Iter type declaration */
extern PyTypeObject diggle_IterType;
 
/* Env type declaration */
extern PyTypeObject diggle_EnvType;

/* Txn type declaration */
extern PyTypeObject diggle_TxnType;

/* Segment type declaration */
extern PyTypeObject diggle_SegmentType;

/* TOC type declaration */
extern PyTypeObject diggle_TOCType;


/* 
 * exception type declarations
 */

extern PyObject *diggle_Exc_Deadlock;
extern PyObject *diggle_Exc_LockTimeout;
extern PyObject *diggle_Exc_DocExists;
extern PyObject *diggle_Exc_Error;


/*
 * iteration type constants
 */
#define DIGGLE_ITER_ALL         1
#define DIGGLE_ITER_RANGE       2
#define DIGGLE_ITER_FROM        3

/* logging levels */
/*
 * these values correspond to the logging levels in the 'logging' package
 * in the python standard library.  for more information see
 * http://docs.python.org/library/logging.html#logging-levels.
 */
#define DIGGLE_LOG_CRITICAL         50
#define DIGGLE_LOG_ERROR            40
#define DIGGLE_LOG_WARNING          30
#define DIGGLE_LOG_INFO             20
#define DIGGLE_LOG_DEBUG            10
#define DIGGLE_LOG_NOTSET           0

/* the size of a diggle_DID_string buffer, including the trailing '\0' */
#define DIGGLE_DID_STRING_LEN       17

#endif
