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
#
# ---------------------------------------------------------------------
#
# This file contains portions of code Copyright 2009 Matt Chaput
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pickle, time
import datetime, dateutil.parser, dateutil.tz
from zope.interface import implements
from terane.bier.index import IIndex
from terane.outputs.store import backend
from terane.outputs.store.schema import Schema
from terane.outputs.store.searching import IndexSearcher
from terane.outputs.store.writing import IndexWriter
from terane.outputs.store.encoding import json_encode, json_decode
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.index')

class Segment(backend.Segment):
    def __init__(self, txn, index, segmentId):
        backend.Segment.__init__(self, txn, index, segmentId)
        self._name = "%s.%i" % (index.name, segmentId)
    def __str__(self):
        return "<terane.outputs.store.Segment '%s'>" % self._name

class Index(backend.Index):
    """
    Stores events, which are a collection of fields.  Internally, an Index is
    made up of multiple Segments.  Instantiation opens the Index, creating it
    if necessary.  The index will be created in the specified environment, and
    thus protected transactionally.

    :param env: The DB environment.
    :type env: :class:`terane.db.backend.Env`
    :param name: The name of the index.
    :type name: str
    :param ids: The ID generator to use for allocating new document identifiers.
    :type ids: :class:`terane.db.idgen.IDGenerator`
    """

    implements(IIndex)

    def __init__(self, env, name, ids):
        backend.Index.__init__(self, env, name)
        try:
            self.name = name
            self._ids = ids
            # load the table of contents
            # load schema
            self._schema = Schema(self)
            self._segments = []
            self._indexsize = 0
            self._currsize = 0
            self._lastmodified = 0
            self._lastid = 0
            # load data segments
            with self.new_txn() as txn:
                for segmentid in self.iter_segments(txn):
                    segment = Segment(txn, self, segmentid)
                    last_update = json_decode(segment.get_meta(txn, 'last-update'))
                    self._currsize = last_update['size']
                    self._indexsize += last_update['size']
                    if last_update['last-id'] > self._lastid:
                        self._lastid = last_update['last-id']
                    if last_update['last-modified'] > self._lastmodified:
                        self._lastmodified = last_update['last-modified']
                    self._segments.append((segment, segmentid))
            if self._segments == []:
                with self.new_txn() as txn:
                    segmentid = self.new_segment(txn)
                    segment = Segment(txn, self, segmentid)
                    segment.set_meta(txn, 'created-on', json_encode(int(time.time())))
                    last_update = {'size': 0, 'last-id': 0, 'last-modified': 0}
                    segment.set_meta(txn, 'last-update', json_encode(last_update))
                self._segments.append((segment, segmentid))
                logger.info("created first segment for new index '%s'" % name)
            else:
                logger.info("found %i documents in %i segments for index '%s'" % (
                    self._indexsize, len(self._segments), name))
            logger.debug("last document id is %s" % self._lastid)
            # get a reference to the current segment
            self._current = self._segments[-1]
            logger.debug("opened event index '%s'" % self.name)
        except:
            self.close()
            raise

    def __str__(self):
        return "<terane.outputs.store.Index '%s'>" % self.name

    def schema(self):
        return self._schema
 
    def searcher(self):
        """
        Return a new object implementing ISearcher, which is protected by a new
        transaction.
        """
        return IndexSearcher(self._current[0])
    
    def writer(self):
        """
        Return a new object implementing IWriter, which is protected by a new
        transaction.
        """
        return IndexWriter(self)

    def newDocumentId(self, ts):
        return self._ids.allocate(ts)

    def segments(self):
        """
        Return a list of (Segment, segment id) tuples.
        """
        return list(self._segments)

    def rotate(self):
        """
        Allocate a new Segment, making it the new current segment.
        """
        with self.new_txn() as txn:
            segmentid = self.new_segment(txn)
            segment = Segment(txn, self, segmentid)
            segment.set_meta(txn, 'created-on', json_encode(int(time.time())))
            last_update = {'size': 0, 'last-id': 0, 'last-modified': 0}
            segment.set_meta(txn, 'last-update', json_encode(last_update))
        self._segments.append((segment,segmentid))
        self._current = (segment,segmentid)
        self._currsize = 0
        logger.debug("rotated current segment, new segment is %s.%i" % (self.name, segmentid))

    def optimize(self, segment):
        """
        Optimize the specified Segment on-disk.
        """
        raise NotImplemented("Index.optimize() not implemented")

    def delete(self, segment, segmentid):
        """
        Delete the specified Segment.
        """
        # remove the segment from the segment list
        self._segments.remove((segment,segmentid))
        # remove the segment from the TOC.  this also marks the segment
        # for eventual physical deletion, when the Segment is deallocated.
        with self.new_txn() as txn:
            self.delete_segment(txn, segmentid)
        segment.delete()
        logger.debug("deleted segment %s.%i" % (self.name, segmentid))

    def close(self):
        """
        Release all resources related to the store.  After calling this method
        the Index instance cannot be used anymore.
        """
        # close each underlying segment
        for segment,segmentid in self._segments:
            segment.close()
        self._segments = list()
        # unref the schema
        self._schema = None
        # close the index
        backend.Index.close(self)
        logger.debug("closed event index '%s'" % self.name)
