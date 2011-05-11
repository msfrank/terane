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

from terane.plugins import Plugin
from terane.outputs import Output
from terane.db import db
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store')

class StoreOutputPlugin(Plugin):

    def configure(self, section):
        pass

    def instance(self):
        return StoreOutput()

class StoreOutput(Output):

    def configure(self, section):
        self._indexName = section.getString("index name", self.name)
        self._segRetention = section.getInt("segment retention policy", 0)
        self._indexRetention = section.getInt("index retention policy", 0)
        self._segOptimize = section.getInt("segment optimization policy", 0)
        
    def startService(self):
        self._index = db.getIndex(self._indexName)
        Output.startService(self)

    def stopService(self):
        self._index = None
        Output.stopService(self)

    def receiveEvent(self, fields):
        # if the output is not running, discard any received events
        if not self.running:
            return
        # remove any fields starting with '_'
        remove = [k for k in fields.keys() if k.startswith('_')]
        for key in remove:
            del fields[key]
        # if the current segment contains more events than specified by
        # _segRetention, then rotate the index to generate a new segment.
        segment = self._index.current()
        if self._segRetention > 0 and segment.count_docs() > self._segRetention:
            self._index.rotate()
        # if the current segment contains more events than specified by
        # _segOptimize, then optimize the segment.
        if self._segOptimize > 0 and segment.count_docs() > self._segOptimize:
            segment.optimize()
        # FIXME: if the index contains more events than specified by _indexRetention,
        # then delete the oldest segment.
        #if self._indexRetention > 0 and self._index.doc_count() > self._indexRetention:
        #    self._index.delete(self._index.segment(0))
        # store the event in the index
        logger.debug("[output:%s] storing event: %s" % (self.name,str(fields)))
        self._index.add(fields)
