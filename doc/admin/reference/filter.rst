===========================
``[filter:]`` Typed Section
===========================

A ``[filter:]`` typed section defines a filter to be used as part of a route.
Each section has a required parameter called ``type``, which determines
what kind of filter to use. 

``type = apache_combined``
""""""""""""""""""""""""""

Parse events from Apache combined format logs.

``type = apache_common``
""""""""""""""""""""""""

Parse events from Apache common format logs.

``type = dt``
"""""""""""""

Reformat date and time fields.

``type = mysql_server``
"""""""""""""""""""""""

Parse events from the mysql server log.

``type = nagios``
"""""""""""""""""

Parse events from the nagios server log.

``type = regex``
""""""""""""""""

Transform abitrary fields based on a regular expression.

``type = syslog``
"""""""""""""""""

Parse syslog formatted events.
