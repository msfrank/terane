# Copyright 2010,2011 Michael Frank <msfrank@syntaxjockey.com>
#
# This file is part of Terane.
#
# Terane is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# Terane is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with Terane.  If not, see <http://www.gnu.org/licenses/>.

import os, fcntl
from terane.outputs.store import backend
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.backend.env')

class Env(backend.Env):
    """
    """

    def __init__(self, dbdir, options=dict()):
        # create berkeleydb-specific directories under the dbdir root
        self.dbdir = dbdir
        if not os.path.exists(self.dbdir):
            os.mkdir(dbdir)
        datadir = os.path.join(self.dbdir, "data")
        if not os.path.exists(datadir):
            os.mkdir(datadir)
        envdir = os.path.join(self.dbdir, "env")
        if not os.path.exists(envdir):
            os.mkdir(envdir)
        tmpdir = os.path.join(self.dbdir, "tmp")
        if not os.path.exists(tmpdir):
            os.mkdir(tmpdir)
        # lock the database directory
        try:
            try:
                self._lock = os.open(os.path.join(self.dbdir, 'lock'), os.O_WRONLY | os.O_CREAT, 0600)
                fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError, e:
                from errno import EACCES, EAGAIN
                if e.errno in (EACCES, EAGAIN):
                    raise Exception("database directory is already locked")
        except Exception, e:
            raise Exception("failed to lock the database directory: %s" % e)
        # open the db environment
        backend.Env.__init__(self, envdir, datadir, tmpdir, options)

    def close(self):
        # close the DB environment
        backend.Env.close(self)
        # unlock the database directory
        try:
            if self._lock != None:
                fcntl.flock(self._lock, fcntl.LOCK_UN | fcntl.LOCK_NB)
        except Exception, e:
            raise Exception("failed to unlock the database directory: %s" % e)
