===========================
``[output:]`` Typed Section
===========================

An ``[output:]`` typed section defines an output to be used as part of a route.
Each section has a required parameter called ``type``, which determines
what kind of filter to use. 

``type = forward``
""""""""""""""""""

Send events to another Terane server.

===================== ======= ==================================================
Configuration Key     Type    Value
===================== ======= ==================================================
forwarding server     string  The address of the remote server to connect to.
forwarding port       integer The port on the remote server to connect to.
retry interval        integer The amount of time to wait between retries if the
                              connection to the remote server is lost.
===================== ======= ==================================================

``type = store``
""""""""""""""""

Store events in a searchable index.

======================== ======= ===============================================
Configuration Key        Type    Value
======================== ======= ===============================================
index name               string  The name of the index.  The default is to use
                                 the name of the output.
segment rotation policy  integer The number of events to store in a single index
                                 segment before creating a new segment.  The
                                 default is 0, which means never rotate
                                 segments.
segment retention policy integer The number of index segments to keep.  The
                                 default is 0, which means never delete a
                                 segment.
optimize segments        boolean If true, then optimize segments after rotation.
======================== ======= ===============================================
