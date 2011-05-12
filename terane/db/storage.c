#include "storage.h"

/* storage module function table */
static PyMethodDef storage_functions[] =
{
    { "get_logfd", terane__get_logfd, METH_NOARGS,
        "Return the reading end of the logger channel." },
    { NULL, NULL, 0, NULL }
};

PyObject *terane_Exc_Deadlock = NULL;
PyObject *terane_Exc_LockTimeout = NULL;
PyObject *terane_Exc_DocExists = NULL;
PyObject *terane_Exc_Error = NULL;

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
    if (PyType_Ready (&terane_EnvType) < 0)
        return;
    if (PyType_Ready (&terane_TOCType) < 0)
        return;
    if (PyType_Ready (&terane_SegmentType) < 0)
        return;
    if (PyType_Ready (&terane_TxnType) < 0)
        return;
    if (PyType_Ready (&terane_IterType) < 0)
        return;

    /* initialize the storage module */
    m = Py_InitModule3 ("storage", storage_functions, "Manipulate the terane database");

    /* load the types into the module */
    Py_INCREF (&terane_EnvType);
    PyModule_AddObject (m, "Env", (PyObject *) &terane_EnvType);
    Py_INCREF (&terane_TOCType);
    PyModule_AddObject (m, "TOC", (PyObject *) &terane_TOCType);
    Py_INCREF (&terane_SegmentType);
    PyModule_AddObject (m, "Segment", (PyObject *) &terane_SegmentType);
    Py_INCREF (&terane_TxnType);
    PyModule_AddObject (m, "Txn", (PyObject *) &terane_TxnType);
    Py_INCREF (&terane_IterType);
    PyModule_AddObject (m, "Iter", (PyObject *) &terane_IterType);

    /* create exceptions */
    terane_Exc_Deadlock = PyErr_NewException("storage.Deadlock", NULL, NULL);
    Py_INCREF (terane_Exc_Deadlock);
    PyModule_AddObject (m, "Deadlock", terane_Exc_Deadlock);

    terane_Exc_LockTimeout = PyErr_NewException("storage.LockTimeout", NULL, NULL);
    Py_INCREF (terane_Exc_LockTimeout);
    PyModule_AddObject (m, "LockTimeout", terane_Exc_LockTimeout);

    terane_Exc_DocExists = PyErr_NewException("storage.DocExists", NULL, NULL);
    Py_INCREF (terane_Exc_DocExists);
    PyModule_AddObject (m, "DocExists", terane_Exc_DocExists);

    terane_Exc_Error = PyErr_NewException("storage.Error", NULL, NULL);
    Py_INCREF (terane_Exc_Error);
    PyModule_AddObject (m, "Error", terane_Exc_Error);
}
