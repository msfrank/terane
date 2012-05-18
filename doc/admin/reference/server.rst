====================
``[server]`` Section
====================

The ``[server]`` section contains global server configuration.

===================== ======= ===============================================
Key                   Type    Value
===================== ======= ===============================================
runtime user          string  The user to switch to on startup.  The user
                              may be specified by name or by UID.
runtime group         string  The group to switch to on startup.  The group
                              may be specified by name or by GID.
log file              path    The path to the server log file.
log verbosity         string  The minimum level for which server messages
                              will be logged to the log file.  May be one of
                              TRACE, DEBUG, INFO, WARNING, ERROR.
log config file       path    The path to the log configuration file, which
                              defines what messages will be logged in a more
                              granular fashion.
pid file              path    The path to the pid file, which terane-server
                              uses to store the current process ID.
auth password file    path    The path to the file storing users and their
                              associated password and roles.
auth permissions file path    The path to the file storing access-control
                              lists for each role.
stats file            path    The path to the file storing server statistics.
stats sync interval   integer The frequency in which statistics are synced to
                              the stats file.
===================== ======= ===============================================
