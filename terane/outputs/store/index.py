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

import time, datetime
from zope.interface import implements
from terane.bier import IIndex
from terane.outputs.store import backend
from terane.outputs.store.segment import Segment
from terane.outputs.store.schema import Schema
from terane.outputs.store.searching import IndexSearcher
from terane.outputs.store.writing import IndexWriter
from terane.outputs.store.encoding import json_encode, json_decode
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.index')

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
    """

    implements(IIndex)

    def __init__(self, env, name, fieldstore):
        self.name = name
        self._segments = []
        self._indexSize = 0
        self._currentSize = 0
        self._lastModified = 0
        self._lastId = 0
        backend.Index.__init__(self, env, name)
        try:
            # load schema
            self._schema = Schema(self, fieldstore)
            # load data segments
            with self.new_txn() as txn:
                for segmentId in self.iter_segments(txn):
                    segment = Segment(txn, self, segmentId)
                    last_update = json_decode(segment.get_meta(txn, 'last-update'))
                    self._currentSize = last_update['size']
                    self._indexSize += last_update['size']
                    if last_update['last-id'] > self._lastId:
                        self._lastId = last_update['last-id']
                    if last_update['last-modified'] > self._lastModified:
                        self._lastModified = last_update['last-modified']
                    self._segments.append(segment)
            # if the index has no segments, create one
            if self._segments == []:
                with self.new_txn() as txn:
                    segmentId = self.new_segment(txn)
                    segment = Segment(txn, self, segmentId)
                    segment.set_meta(txn, 'created-on', json_encode(int(time.time())))
                    last_update = {'size': 0, 'last-id': 0, 'last-modified': 0}
                    segment.set_meta(txn, 'last-update', json_encode(last_update))
                self._segments.append(segment)
                logger.info("created first segment for new index '%s'" % name)
            else:
                logger.info("found %i documents in %i segments for index '%s'" % (
                    self._indexSize, len(self._segments), name))
            logger.debug("last document id is %s" % self._lastId)
            # get a reference to the current segment
            self._current = self._segments[-1]
            logger.debug("opened event index '%s'" % self.name)
        except:
            self.close()
            raise

    def __str__(self):
        return "<terane.outputs.store.Index '%s'>" % self.name

    def getSchema(self):
        return self._schema
 
    def newSearcher(self):
        """
        Return a new object implementing ISearcher.
        """
        return IndexSearcher(self)
    
    def newWriter(self):
        """
        Return a new object implementing IWriter, which is protected by a new
        transaction.
        """
        return IndexWriter(self)

    def getStats(self):
        """
        """
        lastModified = datetime.datetime.fromtimestamp(self._lastModified).isoformat()
        return {
            "index-size": self._indexSize,
            "current-segment-size": self._currentSize,
            "num-segments": len(self._segments),
            "last-modified": lastModified,
            "last-event": self._lastId
            }

    def rotateSegments(self, segRotation, segRetention):
        """
        Allocate a new Segment, making it the new current segment.
        """
        # if the current segment contains more events than specified by
        # segRotation, then rotate the index to generate a new segment.
        if segRotation > 0 and self._currentSize >= segRotation:
            with self.new_txn() as txn:
                segmentId = self.new_segment(txn)
                segment = Segment(txn, self, segmentId)
                segment.set_meta(txn, 'created-on', json_encode(int(time.time())))
                last_update = {'size': 0, 'last-id': 0, 'last-modified': 0}
                segment.set_meta(txn, 'last-update', json_encode(last_update))
            self._segments.append(segment)
            self._current = segment
            self._currentSize = 0
            logger.debug("rotated current segment, new segment is %s" % segment.fullName)
            # if the index contains more segments than specified by segRetention,
            # then delete the oldest segment.
            if segRetention > 0:
                if len(self._segments) > segRetention:
                    for segment in self._segments[0:len(self._segments)-segRetention]:
                        self.delete(segment)

    def delete(self, segment):
        """
        Delete the specified Segment.
        """
        fullName = segment.fullName
        # remove the segment from the segment list
        self._segments.remove(segment)
        # remove the segment from the TOC.  this also marks the segment
        # for eventual physical deletion, when the Segment is deallocated.
        with self.new_txn() as txn:
            self.delete_segment(txn, segment.segmentId)
        segment.delete()
        logger.debug("deleted segment %s" % fullName)

    def close(self):
        """
        Release all resources related to the store.  After calling this method
        the Index instance cannot be used anymore.
        """
        # close each underlying segment
        for segment in self._segments:
            segment.close()
        self._segments = list()
        # unref the schema
        self._schema = None
        # close the index
        backend.Index.close(self)
        logger.debug("closed event index '%s'" % self.name)
