=================
Installing Terane
=================

-------------
Prerequisites
-------------

Terane has the following runtime dependencies:

* `python`_: Terane requires at least version 2.6.  However, python3 is also not supported.  Terane is tested on version 2.6.5.
* `setuptools`_: Any recent version should work.  Terane is tested with version 0.6.10.
* `twisted`_: Any recent version should work.  Terane is tested with version 10.0.0.
* `Berkeley DB`_: Any version in the 4.x/5.x series should work.  Terane is tested with version 4.8.
* `python-dateutil`_: Any recent version should work.  Terane is tested with version 1.4.1.
* `pyparsing`_: Any recent version should work.  Terane is tested with version 1.5.2.
* `urwid`_: Any recent version should work.  Terane is tested with version 0.9.9.1.

In order to build Terane from source, there are additional requirements.  The
storage backend is written in C, so you will also need a basic C development
environment, including the compiler toolchain and C library headers, as well
as development headers for python and Berkeley DB.  Finally, in order to get
the latest source code from source control you will need `git`_ version control
tool.

It is usually easiest to install all prerequisites from distribution packages.
See the page on :doc:`platform-specific notes <platforms>` for details.

.. _python: http://www.python.org
.. _setuptools: http://pypi.python.org/pypi/setuptools
.. _twisted: http://www.twistedmatrix.com
.. _Berkeley DB: http://www.oracle.com/technetwork/database/berkeleydb
.. _python-dateutil: http://niemeyer.net/python-dateutil
.. _pyparsing: http://pyparsing.wikispaces.com
.. _urwid: http://excess.org/urwid
.. _git: http://git-scm.com

--------------
Get the source
--------------

Terane source is currently available from github.  To get the latest source code
from the master branch, run the following command to clone the repository::

 $ git clone git://github.com/msfrank/terane.git

Alternatively, downloads of tagged releases are available as zip or gzipped tar
archives at https://github.com/msfrank/terane/tags.

-----------------
Build and install
-----------------

If you downloaded a release archive, then extract the archive contents.  Next,
change into the top-level source directory.  Compilation and installation can be
wrapped up in a simple step::

 $ cd terane
 $ sudo ./setup.py install

This will compile extension modules, compile all .py files into bytecode, and
install the entire application into ``/usr/local``.  If you want to install
Terane into a different location, pass a ``--prefix`` option to the install
line above.  For example, to install into the standard system directory ``/usr``,
run the following command::

 $ sudo ./setup.py install --prefix /usr

For the remainder of the documentation, it will be assumed that terane has been
installed into ``/usr``.

----------------------------------
Create a user and group for terane
----------------------------------

It is considered good security practice to use role-based access control (RBAC) to limit
the ability of software to perform operations outside of its scope.  It is recommended
that a new user and group be created for use by terane, and that the terane user be added
to other groups as necessary to access resources such as log file inputs.

--------------------------
Create runtime directories
--------------------------

Terane expects certain directories to be present at runtime:

=================== ========================================
Path                Description
=================== ========================================
``/etc/terane``     Where configuration is read from.
``/var/lib/terane`` Where the storage module writes data to.
``/var/log/terane`` Where server logs are written to.
``/var/run/terane`` Where the server pid file is written to.
=================== ========================================

Each directory specified above should be owned by the terane user and group.  For the
directory ``/var/lib/terane``, it is recommended that the directory mode be set so *only*
the user and group have any access permissions.
