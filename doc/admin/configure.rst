==================
Configuring Terane
==================

-------------------------------
Configuration file syntax rules
-------------------------------

By default, ``terane-server`` looks for its configuration file in
``/etc/terane/terane-server.conf``.  The server configuration file uses an
INI-file format, which has sections composed of key-value pairs.  A section is
declared by a bracket syntax, with the section declaration on a single line by
itself.  The following declares a section called 'mysection'::

 [mysection]

A section has zero or more key-value pairs associated with it.  A key is an
alphanumeric string with spaces allowed.  The key is separated from the value
by an equals-sign '=' or a colon ':'.  The value is everything after the
separator, with leading and trailing whitespace trimmed.  The following
declares a section 'mysection' containing a key 'foo' with the value 'bar'::

 [mysection]
 foo = bar

-----------------------------
Server configuration sections
-----------------------------


-------------------------------
An example server configuration
-------------------------------

Below is a basic but complete configuration, annotated with comments::

 [server]
 
 [plugin:protocol:xmlrpc]
 listen address = 127.0.0.1
 listen port = 45565
 
 [plugin:input:file]
 
 [plugin:filter:syslog]
 
 [plugin:output:store]
 data directory = /var/lib/terane/db
 
 [input:messages]
 type = file
 file path = /var/log/messages
 
 [filter:syslog]
 type = syslog
 
 [output:local]
 type = store
 segment rotation policy = 10000
 segment retention policy = 50
 
 [route:messages_to_local]
 input = messages
 filter = syslog
 output = local
