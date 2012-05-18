===========================
``[plugin:]`` Typed Section
===========================

A ``[plugin:]`` typed section defines a plugin to load.

----------------
Protocol Plugins
----------------

``[plugin:protocol:xmlrpc]``
""""""""""""""""""""""""""""

Provides an XMLRPC interface to the Terane server, which the Terane client
tools use to interact with the server.

===================== ======= ===============================================
Configuration Key     Type    Value
===================== ======= ===============================================
listen address        string  The network address to bind to.  The default is
                              to bind to all available interfaces.
listen port           integer The network port to bind to.  The default is to
                              bind to port 45565.
===================== ======= ===============================================

-------------
Input Plugins
-------------

``[plugin:input:file]``
"""""""""""""""""""""""

Monitor files.

===================== ======= ===============================================
Configuration Key     Type    Value
===================== ======= ===============================================
===================== ======= ===============================================

``[plugin:input:syslog]``
"""""""""""""""""""""""""

Listen for syslog messages over UDP.

===================== ======= ===============================================
Configuration Key     Type    Value
===================== ======= ===============================================
syslog udp address    string  The network address to bind to.  The default is
                              to bind to all available interfaces.
syslog udp port       integer The network port to bind to.  The default is to
                              bind to port 514.
===================== ======= ===============================================

``[plugin:input:collect]``
""""""""""""""""""""""""""

Listen for events from other terane servers.

===================== ======= ===============================================
Configuration Key     Type    Value
===================== ======= ===============================================
collect address       string  The network address to bind to.  The default is
                              to bind to all available interfaces.
collect port          integer The network port to bind to.  The default is to
                              bind to port 8643.
===================== ======= ===============================================

--------------
Filter Plugins
--------------

``[plugin:filter:apache]``
""""""""""""""""""""""""""

Parse events from Apache common- and combined-format logs.

``[plugin:filter:dt]``
""""""""""""""""""""""

``[plugin:filter:mysql]``
"""""""""""""""""""""""""

Parse events from the mysql server log.

``[plugin:filter:nagios]``
""""""""""""""""""""""""""

Parse events from the nagios server log.

``[plugin:filter:regex]``
"""""""""""""""""""""""""

Transform abitrary fields based on a regular expression.

``[plugin:filter:syslog]``
""""""""""""""""""""""""""

Parse syslog formatted events.
