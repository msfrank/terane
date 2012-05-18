
Terane |release| Manual
=======================

Terane is a search engine for logs.  Terane consumes log events from
multiple sources (log files, syslog messages, other Terane servers), 
analyzes them, and feeds them into a network-aware searchable database.

Terane is licensed under the GPL version 3 or later.

Documentation is split up in to three section, depending on the audience.
User Documentation describes how to use the terane client software to
query the database.  Administrator Documentation is geared towards system
administrators, and describes how to install, configure, and maintain the
server software.  Developer Documentation delves into the architecture of
the server, and contains extensive API documentation.

.. toctree::
   :maxdepth: 2

   why
   features
   support
   links


User Documentation
------------------

.. toctree::
   :maxdepth: 2

   user/toolbox
   user/ql
   user/console
   user/search
   user/tail
   user/grok
   user/configure


Administrator Documentation
---------------------------

.. toctree::
   :maxdepth: 2

   admin/install
   admin/configure
   admin/platforms


Developer Documentation
-----------------------

.. toctree::
   :glob:

   devel/architecture
   devel/internals
