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
from whoosh.index import Index as WhooshIndex
from whoosh.fields import FieldType, TEXT, DATETIME, NUMERIC
from whoosh.analysis import SimpleAnalyzer
from whoosh.searching import Searcher as WhooshSearcher
from whoosh.qparser import QueryParser
from terane.outputs.store.backend import TOC, Segment
from terane.outputs.store.schema import Schema
from terane.outputs.store.reading import MultiReader
from terane.outputs.store.writing import IndexWriter
from terane.outputs.store.encoding import json_encode, json_decode
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.index')

class Index(WhooshIndex):
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

    def __init__(self, env, name, ids):
        try:
            self._env = env
            self.name = name
            self._ids = ids
            # load the table of contents
            self._toc = TOC(self._env, self.name)
            # load schema
            self.schema = Schema(self._toc)
            self._segments = []
            self._indexsize = 0
            self._currsize = 0
            self._lastmodified = 0
            self._lastid = 0
            # load data segments
            with self._toc.new_txn() as txn:
                for segmentid in self._toc.iter_segments(txn):
                    segment = Segment(txn, self._toc, segmentid)
                    last_update = json_decode(segment.get_meta(txn, 'last-update'))
                    self._currsize = last_update['size']
                    self._indexsize += last_update['size']
                    if last_update['last-id'] > self._lastid:
                        self._lastid = last_update['last-id']
                    if last_update['last-modified'] > self._lastmodified:
                        self._lastmodified = last_update['last-modified']
                    self._segments.append((segment, segmentid))
            if self._segments == []:
                with self._toc.new_txn() as txn:
                    segmentid = self._toc.new_segment(txn)
                    segment = Segment(txn, self._toc, segmentid)
                    segment.set_meta(txn, 'created-on', json_encode(int(time.time())))
                    last_update = {'size': 0, 'last-id': 0, 'last-modified': 0}
                    segment.set_meta(txn, 'last-update', json_encode(last_update))
                self._segments.append((segment, segmentid))
                logger.info("created first segment for new index '%s'" % name)
            else:
                logger.info("found %i documents in %i segments for index '%s'" % (
                    self._indexsize, self._toc.count_segments(), name))
            logger.debug("last document id is %s" % self._lastid)
            # get a reference to the current segment
            self._current = self._segments[-1]
            # check for the ts field
            if 'ts' in self.schema:
                ts = self.schema['ts']
                if not isinstance(ts, DATETIME):
                    raise Exception("Index schema contains invalid 'ts' field")
            # if the ts field doesn't exist, then add it
            else:
                self.schema.add('ts', DATETIME(stored=True))
            # check for the id field
            if 'id' in self.schema:
                id = self.schema['id']
                if not isinstance(ts, NUMERIC):
                    raise Exception("Index schema contains invalid 'id' field")
            # if the id field doesn't exist, then add it
            else:
                self.schema.add('id', NUMERIC(long, stored=True))
            logger.debug("opened event index '%s'" % self.name)
        except:
            self.close()
            raise

    def last_id(self):
        """
        Return the document ID of the last document added to the index.

        :returns: The last document ID.
        :rtype: long.
        """
        return self._lastid

    def last_modified(self):
        """
        Return the date when the index was last modified, in seconds since
        the epoch (as returned by time.time().

        This method overrides WhooshIndex.last_modified().

        :returns: The last-modified date in seconds since the epoch.
        :rtype: int.
        """
        return self._lastmodified

    def doc_count_all(self):
        """
        Return the total number of documents in the index.

        This method overrides WhooshIndex.doc_count_all().

        :returns: The total number of documents in the index.
        :rtype: long.
        """
        return self._indexsize
    
    def doc_count(self):
        """
        Return the total number of documents in the index.  This returns the
        same value as doc_count_all().  the difference is that doc_count()
        returns the number of undeleted documents, while doc_count_all()
        returns the total number of documents.  since we don't keep track of
        deleted documents (we just delete em :) these two values are always the
        same.

        This method overrides WhooshIndex.doc_count().

        :returns: The total number of documents in the index.
        :rtype: long.
        """
        return self._indexsize

    def is_empty(self):
        """
        Return True if no documents are in the index.

        This method overrides WhooshIndex.is_empty().
        """
        if self._indexsize == 0:
            return True
        return False
       
    def reader(self):
        """
        Return a new Reader object instance, which is protected by a new
        transaction.

        This method overrides WhooshIndex.reader().
        """
        return MultiReader(self)
    
    def writer(self):
        """
        Return a new Writer object instance, which is protected by a new
        transaction.

        This method overrides WhooshIndex.writer().
        """
        return IndexWriter(self)

    def add(self, fields):
        """
        Add event to the current segment.
        """
        _fields = {}
        # if id field is specified, then toss it
        if 'id' in fields:
            del fields['id']
        # if timestamp is specified, then convert it to a datetime object if needed
        if 'ts' in fields:
            ts = fields['ts']
            del fields['ts']
            # if the ts field is not a datetime, then parse its string representation
            if not isinstance(ts, datetime.datetime):
                ts = dateutil.parser.parse(str(ts))
            # if no timezone is specified, then assume local tz
            if ts.tzinfo == None:
                ts = ts.replace(tzinfo=dateutil.tz.tzlocal())
            # convert to UTC, if necessary
            if not ts.tzinfo == dateutil.tz.tzutc():
                ts = ts.astimezone(dateutil.tz.tzutc())
            _fields['ts'] = ts
        # otherwise if there is no timestamp, then generate one
        else:
            _fields['ts'] = datetime.datetime.now(dateutil.tz.tzutc())
        # make the timestamp timezone-naive so Whoosh can handle it
        _fields['ts'] = _fields['ts'].replace(tzinfo=None) - _fields['ts'].utcoffset()
        # set the stored value of the 'ts' field to a pretty string
        _fields['&ts'] = _fields['ts'].isoformat()
        # make sure every other field exists in the schema, and is a unicode string
        for key,value in fields.items():
            _fields[key] = unicode(value)
            # if the field doesn't exist in the schema, then add it
            if not key in self.schema:
                self.schema.add(key, TEXT(SimpleAnalyzer(), stored=True))
        # write the fields to the index
        with self.writer() as writer:
            lastid, lastmodified = writer.add_document(**_fields)
        self._lastid = lastid
        self._lastmodified = lastmodified
        self._currsize += 1
        self._indexsize += 1

    def search(self, query, limit=100, sortedby='ts', reverse=False):
        """
        Search the index using the specified whoosh.Query.
        """
        searcher = self.searcher()
        return searcher.search(query, limit, sortedby, reverse)

    def segments(self):
        """
        Return a list of (Segment, segment id) tuples.
        """
        return list(self._segments)

    def rotate(self):
        """
        Allocate a new Segment, making it the new current segment.
        """
        with self._toc.new_txn() as txn:
            segmentid = self._toc.new_segment(txn)
            segment = Segment(self._toc, segmentid)
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

        This method overrides WhooshIndex.optimize().
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
        with self._toc.new_txn() as txn:
            self._toc.delete_segment(txn, segmentid)
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
        # close the TOC
        self._toc.close()
        self._toc = None
        # unref the environment
        self._env = None
        # unref the schema
        self.schema = None
        logger.debug("closed event index '%s'" % self.name)
