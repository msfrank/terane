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
from zope.interface import implements
from zope.component import getUtility
from terane.plugins import Plugin, IPlugin
from terane.sched import IScheduler
from terane.bier.event import Contract
from terane.bier.writing import WriterWorker
from terane.outputs import Output, IOutput, ISearchable
from terane.outputs.store.env import Env
from terane.outputs.store.index import Index
from terane.outputs.store.logfd import LogFD
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store')

class StoreOutput(Output):

    implements(IOutput, ISearchable)

    def __init__(self, plugin, name, fieldstore):
        self._plugin = plugin
        self.setName(name)
        self._fieldstore = fieldstore
        self._index = None
        self._contract = Contract().sign()
        self._task = getUtility(IScheduler).addTask("output:%s" % name)

    def configure(self, section):
        self._indexName = section.getString("index name", self.name)
        if self._indexName in self._plugin._outputs:
            raise Exception("[output:%s] index '%s' is already open" % (self.name,self._indexName))
        self._plugin._outputs[self._indexName] = self
        self._segRotation = section.getInt("segment rotation policy", 0)
        self._segRetention = section.getInt("segment retention policy", 0)
        self._segOptimize = section.getBoolean("optimize segments", False)
        
    def startService(self):
        self._index = Index(self._plugin._env, self._indexName, self._fieldstore)
        logger.debug("[output:%s] opened index '%s'" % (self.name,self._indexName))
        Output.startService(self)

    def stopService(self):
        if self._index != None:
            self._index.close()
        logger.debug("[output:%s] closed index '%s'" % (self.name,self._indexName))
        self._index = None
        return Output.stopService(self)

    def getContract(self):
        return self._contract

    def receiveEvent(self, event):
        # if the output is not running, discard any received events
        if not self.running:
            return
        # store the event in the index
        worker = self._task.addWorker(WriterWorker(event, self._index))
        # rotate the index segments if necessary
        d = worker.whenDone()
        d.addCallbacks(self._rotateSegments, self._writeError)
    
    def _rotateSegments(self, worker):
        logger.debug("[output:%s] wrote event to index" % self.name)
        try:
            self._index.rotateSegments(self._segRotation, self._segRetention)
        except Exception, e:
            logger.exception(e)

    def _writeError(self, failure):
        logger.error("[output:%s] failed to write event: %s" % (self.name, failure))

    def getIndex(self):
        return self._index

class StoreOutputPlugin(Plugin):

    implements(IPlugin)

    components = [(StoreOutput, IOutput, 'store')]

    def __init__(self):
        Plugin.__init__(self)
        self._env = None
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
        Plugin.startService(self)

    def stopService(self):
        """
        Stop the database service, closing all open indices.
        """
        Plugin.stopService(self)
        # close the DB environment
        self._env.close()
        self._logfd.stopReading()
        self._logfd = None
        logger.debug("[%s] closed database environment" % self.name)
