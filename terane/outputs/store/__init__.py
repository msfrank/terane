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
from twisted.internet import reactor
from twisted.application.service import MultiService
from terane.plugins import Plugin
from terane.outputs import Output
from terane.outputs.store.storage import Env
from terane.outputs.store.index import Index
from terane.outputs.store.idgen import IDGenerator
from terane.outputs.store.logfd import LogFD
from terane.loggers import getLogger

logger = getLogger('terane.db')

class StoreOutput(Output):

    def __init__(self):
        self._index = None

    def configure(self, section):
        self._indexName = section.getString("index name", self.name)
        self._segRotation = section.getInt("segment rotation policy", 0)
        self._segRetention = section.getInt("segment retention policy", 0)
        self._segOptimize = section.getBoolean("optimize segments", False)
        
    def startService(self):
        if self._indexName in self._plugin._outputs:
            raise Exception("[output:%s] index '%s' is already open" % self._indexName)
        self._index = Index(self.parent._env, self._indexName, self.parent._ids)
        Output.startService(self)

    def stopService(self):
        if self._index != None:
            self._index.close()
        self._index = None
        return Output.stopService(self)

    def receiveEvent(self, fields):
        # if the output is not running, discard any received events
        if not self.running:
            return
        # remove any fields starting with '_'
        remove = [k for k in fields.keys() if k.startswith('_')]
        for key in remove:
            del fields[key]
        # store the event in the index
        logger.trace("[output:%s] storing event: %s" % (self.name,str(fields)))
        self._index.add(fields)
        # if the current segment contains more events than specified by
        # _segRotation, then rotate the index to generate a new segment.
        segment,segmentid = self._index.current()
        if self._segRotation > 0 and segment.count_docs() >= self._segRotation:
            self._index.rotate()
            # if the index contains more segments than specified by _segRetention,
            # then delete the oldest segment.
            if self._segRetention > 0:
                segments = self._index.segments()
                if len(segments) > self._segRetention:
                    for segment,segmentid in segments[0:len(segments)-self._segRetention]:
                        self._index.delete(segment, segmentid)
    
class StoreOutputPlugin(Plugin):

    factory = StoreOutput

    def __init__(self):
        Plugin.__init__(self)
        self._env = None
        self._ids = IDGenerator()
        self._lock = None
        self._outputs = {}

    def configure(self, section):
        """
        Configure the store plugin.
        """
        self.dbdir = os.path.abspath(section.getPath('data directory', '/var/lib/terane/db/'))
        self.cachesize = section.getInt('cache size', 256 * 1024)
        self.ncaches = section.getInt('multiple caches', 1)
        self._ids.configure(section, self.dbdir)

    def startService(self):
        """
        Start the database service.
        """
        # start processing logfd messages
        self._logfd = LogFD()
        self._logfd.startReading()
        # create berkeleydb-specific directories under the dbdir root
        datadir = os.path.join(self.dbdir, "data")
        envdir = os.path.join(self.dbdir, "env")
        tmpdir = os.path.join(self.dbdir, "tmp")
        if not os.path.exists(self.dbdir):
            os.mkdir(self.dbdir)
        if not os.path.exists(datadir):
            os.mkdir(datadir)
        if not os.path.exists(envdir):
            os.mkdir(envdir)
        if not os.path.exists(tmpdir):
            os.mkdir(tmpdir)
        # lock the database directory
        try:
            self._lock = os.open(os.path.join(self.dbdir, 'lock'), os.O_WRONLY | os.O_CREAT, 0600)
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError, e:
            from errno import EACCES, EAGAIN
            if e.errno in (EACCES, EAGAIN):
                raise Exception("Failed to lock the database directory: database is already locked")
        except Exception, e:
            raise Exception("Failed to lock the database directory: %s" % e)
        # open the db environment
        self._env = Env(envdir, datadir, tmpdir, cachesize=self.cachesize)
        logger.debug("[plugin:output:store] opened database environment in %s" % self.dbdir)
        # start the id generator
        self._ids.startService()
        Plugin.startService(self)

    def stopService(self):
        """
        Stop the database service, closing all open indices.
        """
        Plugin.stopService(self)
        # close each open index, and remove reference to it
        for name,output in self._outputs.items():
            if output.running:
                output.stopService()
            del self._outputs[name]
        # stop the id generator
        self._ids.stopService()
        # close the DB environment
        self._env.close()
        # unlock the database directory
        try:
            if self._lock != None:
                fcntl.flock(self._lock, fcntl.LOCK_UN | fcntl.LOCK_NB)
        except Exception, e:
            logger.warning("Failed to unlock the database directory: %s" % e)
        self._lock = None
        logger.debug("closed database environment")
