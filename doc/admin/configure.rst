=========================
Configuring Terane Server
=========================

Terane is a system for indexing and searching events.  An event is an occurance
of something interesting; it contains at a minimum a timestamp (the 'when'), a
hostname (the 'where'), and a message (the 'what'), but may optionally contain
a number of additional fields.

At a fundamental level, the Terane server is a message router, receiving events
from inputs, optionally modifying the events using filters, and storing the
result in outputs.  An input is said to *emit* events.  There are multiple types
of inputs- for example, a ``file`` input watches a file, and emits lines from
the file as they are written.  A filter is said to *transform* events; typically
a filter will extract more meaning from an event input, such as parsing a
particular record format.  Finally, an output is said to *consume* events; this
most likely means storing the event in an index, but alternatively you could
define an output that forwards events to another remote server.

The input-output relationship is many-to-many; that is to say, an event from one
input may be stored in multiple outputs, and conversely, events from multiple
inputs may be stored in one output.  In order to express these relationships,
Terane exposes the concept of a *route*.  A route consists of one input, zero or
more filters, and one output.  Inputs, filters, and outputs may all be reused
in multiple routes.

-------------------------
Configuration File Syntax
-------------------------

By default, ``terane-server`` looks for its configuration file in
``/etc/terane/terane-server.conf``.  The server configuration file uses an
INI-file format, which has sections composed of key-value pairs.  A section is
declared by a bracket syntax, with the section declaration on a single line by
itself.  The following declares a section called 'mysection'::

 [mysection]

Sections may also be 'typed'; that is, the section may be declared as of a
specific type by prefixing the section name with a colon-delimited type.  The
following example declares a section called 'mysection' of type 'mytype'::

 [mytype:mysection]

A section has zero or more key-value pairs associated with it.  Leading and
trailing whitespace in the key is ignored.  The key is separated from the value
by an equals-sign '=' or a colon ':'.  The value is everything after the
separator, with leading and trailing whitespace trimmed.  The following
declares a section 'mysection' containing a key 'foo' with the value 'bar'::

 [mysection]
 foo = bar

A configuration file may include comments.  A comment is prefixed by either the
hash '#' or semicolon ';'. Comments may appear either on their own in an
otherwise empty line, or they may be entered 'in-line' after a key-value or
section name.  An in-line comment must be preceded by whitespace, and may be
started only by a semi-colon.

Finally, order of sections or key-values within a section is unimportant.  While
it is customary (and reads better) to define sections before they are used, Terane
will parse the configuration in the correct order regardless.

------------------------------
Server Configuration Semantics
------------------------------

Terane parses the server configuration file in a specific order.  First, it
looks for the ``[server]`` section, and loads the global server configuration.
Next, each ``[plugin:]`` typed section is read, and each defined plugin is
loaded and configured.  After the plugins are ready, each ``[input:]``,
``[filter:]``, and ``[output:]`` typed section is read, and the defined inputs,
filters, and outputs are loaded and configured.  Finally, each ``[route:]``
typed section is read, and the routes are constructed.

-------------------------------
An example server configuration
-------------------------------

Below is a basic but complete configuration, linked to further reference
material.  A single route is defined which watches /var/log/messages, parses
input events using the syslog filter, and stores them in an index called
'local'::

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

------------------------------
Server Configuration Reference
------------------------------

.. toctree::
   :maxdepth: 2

   reference/server
   reference/plugin
