#include "storage.h"

/* storage module function table */
static PyMethodDef storage_functions[] =
{
    { "get_logfd", diggle__get_logfd, METH_NOARGS,
        "Return the reading end of the logger channel." },
    { NULL, NULL, 0, NULL }
};

PyObject *diggle_Exc_Deadlock = NULL;
PyObject *diggle_Exc_LockTimeout = NULL;
PyObject *diggle_Exc_DocExists = NULL;
PyObject *diggle_Exc_Error = NULL;

/* storage module init function */
PyMODINIT_FUNC
initstorage (void)
{
    PyObject *m;
    int dbret;

    /* set berkeley db to use the python memory allocation functions */
    if ((dbret = db_env_set_func_malloc (PyMem_Malloc)) != 0) {
        PyErr_Format (PyExc_SystemError, "Failed to set internal memory routines: %s", db_strerror (dbret));
        return;
    }
    if ((dbret = db_env_set_func_realloc (PyMem_Realloc)) != 0) {
        PyErr_Format (PyExc_SystemError, "Failed to set internal memory routines: %s", db_strerror (dbret));
        return;
    }
    if ((dbret = db_env_set_func_free (PyMem_Free)) != 0) {
        PyErr_Format (PyExc_SystemError, "Failed to set internal memory routines: %s", db_strerror (dbret));
        return;
    }

    /* verify the object types are ready to load */
    if (PyType_Ready (&diggle_EnvType) < 0)
        return;
    if (PyType_Ready (&diggle_TOCType) < 0)
        return;
    if (PyType_Ready (&diggle_SegmentType) < 0)
        return;
    if (PyType_Ready (&diggle_TxnType) < 0)
        return;
    if (PyType_Ready (&diggle_IterType) < 0)
        return;

    /* initialize the storage module */
    m = Py_InitModule3 ("storage", storage_functions, "Manipulate the diggle database");

    /* load the types into the module */
    Py_INCREF (&diggle_EnvType);
    PyModule_AddObject (m, "Env", (PyObject *) &diggle_EnvType);
    Py_INCREF (&diggle_TOCType);
    PyModule_AddObject (m, "TOC", (PyObject *) &diggle_TOCType);
    Py_INCREF (&diggle_SegmentType);
    PyModule_AddObject (m, "Segment", (PyObject *) &diggle_SegmentType);
    Py_INCREF (&diggle_TxnType);
    PyModule_AddObject (m, "Txn", (PyObject *) &diggle_TxnType);
    Py_INCREF (&diggle_IterType);
    PyModule_AddObject (m, "Iter", (PyObject *) &diggle_IterType);

    /* create exceptions */
    diggle_Exc_Deadlock = PyErr_NewException("storage.Deadlock", NULL, NULL);
    Py_INCREF (diggle_Exc_Deadlock);
    PyModule_AddObject (m, "Deadlock", diggle_Exc_Deadlock);

    diggle_Exc_LockTimeout = PyErr_NewException("storage.LockTimeout", NULL, NULL);
    Py_INCREF (diggle_Exc_LockTimeout);
    PyModule_AddObject (m, "LockTimeout", diggle_Exc_LockTimeout);

    diggle_Exc_DocExists = PyErr_NewException("storage.DocExists", NULL, NULL);
    Py_INCREF (diggle_Exc_DocExists);
    PyModule_AddObject (m, "DocExists", diggle_Exc_DocExists);

    diggle_Exc_Error = PyErr_NewException("storage.Error", NULL, NULL);
    Py_INCREF (diggle_Exc_Error);
    PyModule_AddObject (m, "Error", diggle_Exc_Error);
}
