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

import os
from twisted.internet import reactor
from twisted.application.service import MultiService
from terane.db.storage import Env
from terane.db.index import Index
from terane.db.idgen import IDGenerator
from terane.db.logfd import LogFD
from terane.loggers import getLogger

logger = getLogger('terane.db')

class DatabaseManager(MultiService):
    def __init__(self):
        """
        Initialize the database service.
        """
        MultiService.__init__(self)
        self.setName("database")
        self._env = None
        self._indices = dict()
        self._ids = IDGenerator()

    def configure(self, settings):
        """
        Configure the database service.
        """
        section = settings.section('database')
        self.dbdir = os.path.abspath(section.getPath('data directory', '/var/lib/terane/db/'))
        self.cachesize = section.getInt('cache size', 256 * 1024)
        self.ncaches = section.getInt('multiple caches', 1)
        self._ids.configure(section, self.dbdir)

    def getIndex(self, name):
        """
        Return the specified Index, creating it if necessary.
        """
        if not self.running:
            raise Exception("Database service is not running")
        if name in self._indices:
            return self._indices[name]
        index = Index(self._env, name, self._ids)
        self._indices[name] = index
        return index

    def startService(self):
        """
        Start the database service.

        This method implements IService.startService().
        """
        datadir = os.path.join(self.dbdir, "data")
        envdir = os.path.join(self.dbdir, "env")
        tmpdir = os.path.join(self.dbdir, "tmp")
        # create berkeleydb-specific directories under the dbdir root
        if not os.path.exists(self.dbdir):
            os.mkdir(self.dbdir)
        if not os.path.exists(datadir):
            os.mkdir(datadir)
        if not os.path.exists(envdir):
            os.mkdir(envdir)
        if not os.path.exists(tmpdir):
            os.mkdir(tmpdir)
        # open the db environment
        self._env = Env(envdir, datadir, tmpdir, cachesize=self.cachesize,
            logger=getLogger('terane.db.storage'))
        logger.debug("opened database environment in %s" % self.dbdir)
        # start the id generator
        self._ids.startService()
        # process logfd messages
        self._logfd = LogFD()
        self._logfd.startReading()
        MultiService.startService(self)

    def stopService(self):
        """
        Stop the database service, closing all open indices.

        This method implements IService.stopService().
        """
        MultiService.stopService(self)
        # close each open index, and remove reference to it
        for name, index in self._indices.items():
            index.close()
            del self._indices[name]
        # stop the id generator
        self._ids.stopService()
        # close the DB environment
        self._env.close()
        logger.debug("closed database environment")

db = DatabaseManager()
