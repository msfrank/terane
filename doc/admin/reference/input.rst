==========================
``[input:]`` Typed Section
==========================

An ``[input:]`` typed section defines an input to be used as part of a route.
Each section has a required parameter called ``type``, which determines
what kind of input to use. 

``type = collect``
""""""""""""""""""

Listen for events from other terane servers.

``type = file``
"""""""""""""""

Monitor files.

===================== ======= ===============================================
Configuration Key     Type    Value
===================== ======= ===============================================
file path             path    The path to the file to watch.
polling interval      integer The frequency in which to poll the file for
                              changes, in seconds.  The default value is 5.
maximum line length   integer The maximum length of a single line, in bytes.
                              The default is 1MB.
loop chunk length     integer The maximum amount of data to process in a 
                              single pass.  This value must be greater than
                              or equal to the maximum line length.  The
                              default is 1MB.
===================== ======= ===============================================
 
``type = syslog``
"""""""""""""""""

Listen for syslog messages over UDP.
