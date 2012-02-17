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
from zope.interface import implements
from terane.plugins import Plugin, IPlugin
from terane.bier.writing import writeEventToIndex
from terane.outputs import Output, ISearchableOutput
from terane.outputs.store.env import Env
from terane.outputs.store.index import Index
from terane.outputs.store.idgen import IDGenerator
from terane.outputs.store.logfd import LogFD
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store')

class StoreOutput(Output):

    implements(ISearchableOutput)

    def __init__(self):
        self._index = None

    def configure(self, section):
        self._indexName = section.getString("index name", self.name)
        self._segRotation = section.getInt("segment rotation policy", 0)
        self._segRetention = section.getInt("segment retention policy", 0)
        self._segOptimize = section.getBoolean("optimize segments", False)
        
    def startService(self):
        if self._indexName in self.parent._outputs:
            raise Exception("[output:%s] index '%s' is already open" % (self.name,self._indexName))
        self._index = Index(self.parent._env, self._indexName, self.parent._ids)
        logger.debug("[output:%s] opened index '%s'" % (self.name,self._indexName))
        Output.startService(self)

    def stopService(self):
        if self._index != None:
            self._index.close()
        logger.debug("[output:%s] closed index '%s'" % (self.name,self._indexName))
        self._index = None
        return Output.stopService(self)

    def receiveEvent(self, event):
        # if the output is not running, discard any received events
        if not self.running:
            return
        # store the event in the index
        writeEventToIndex(event, self._index)
        # rotate the index segments if necessary
        self._index.rotateSegments(self._segRotation, self._segRetention)
    
    def index(self):
        return self._index

class StoreOutputPlugin(Plugin):

    implements(IPlugin)

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
        self._dbdir = os.path.abspath(section.getPath('data directory', '/var/lib/terane/db/'))
        self._options = {}
        self._options['cache size'] = long(section.getInt('cache size', 64 * 1024 * 1024))
        self._options['max lockers'] = long(section.getInt('max lockers', 65536))
        self._options['max locks'] = long(section.getInt('max locks', 65536))
        self._options['max objects'] = long(section.getInt('max objects', 65536))
        self._options['max transactions'] = long(section.getInt('max transactions', 0))
        self._ids.configure(section, self._dbdir)

    def startService(self):
        """
        Start the database service.
        """
        # start processing logfd messages
        self._logfd = LogFD()
        self._logfd.startReading()
        # open the db environment
        self._env = Env(self._dbdir, self._options)
        logger.debug("[%s] opened database environment in %s" % (self.name,self._dbdir))
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
        logger.debug("[%s] closed database environment" % self.name)
