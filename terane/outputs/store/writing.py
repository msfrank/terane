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

import time
from zope.interface import implements
from twisted.internet.defer import succeed, fail
from twisted.internet.threads import deferToThread
from terane.bier import IWriter
from terane.bier.writing import WriterError
from terane.outputs.store.schema import Schema
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.writing')

class WriterExpired(WriterError):
    pass

class IndexWriter(object):

    implements(IWriter)

    def __init__(self, ix):
        self._txn = None
        logger.trace("[txn %s] waiting for writeLock" % self)
        with ix._writeLock:
            logger.trace("[txn %s] acquired writeLock" % self)
            self._segment = ix._current
            logger.trace("[txn %s] released writeLock" % self)
        self._ix = ix
        logger.trace("[txn %s] BEGIN new_txn" % self)
        self._txn = ix.new_txn()
        logger.trace("[txn %s] END new_Txn" % self)
        self._numEvents = 0
        self._lastId = None
        self._lastModified = None

    def __str__(self):
        if self._txn:
            return "%s:%x" % (str(hash(self)), self._txn.id())
        return str(hash(self))

    def getSchema(self):
        return succeed(Schema(self._ix, self._txn))

    def newEvent(self, evid, event):
        def _newEvent(writer, evid, event):
            segment = writer._segment
            # serialize the fields dict and write it to the segment
            logger.trace("[txn %s] BEGIN set_event" % writer)
            segment.set_event(writer._txn, [evid.ts,evid.offset], event,
                              NOOVERWRITE=True)
            logger.trace("[txn %s] END set_event" % writer)
            # update segment metadata
            writer._numEvents += 1
            writer._lastId = evid
            writer._lastModified = int(time.time())
            lastUpdate = {
                u'size': writer._ix._currentSize + 1,
                u'last-id': [evid.ts, evid.offset],
                u'last-modified': writer._lastModified
                }
            logger.trace("[txn %s] BEGIN set_meta" % writer)
            segment.set_meta(writer._txn, u'last-update', lastUpdate)
            logger.trace("[txn %s] END set_meta" % writer)
        return deferToThread(_newEvent, self, evid, event)

    def newPosting(self, field, term, evid, posting):
        def _newPosting(writer, field, term, evid, posting):
            segment = writer._segment
            # increment the document count for this field
            f = [field.fieldname, field.fieldtype]
            try:
                logger.trace("[txn %s] BEGIN get_field" % writer)
                value = segment.get_field(writer._txn, f, RMW=True)
                logger.trace("[txn %s] END get_field" % writer)
                if not u'num-docs' in value:
                    raise WriterError("field %s is missing key 'num-docs'" % f)
                value[u'num-docs'] += 1
            except KeyError:
                value = {u'num-docs': 1}
            logger.trace("[txn %s] BEGIN set_field" % writer)
            segment.set_field(writer._txn, f, value)
            logger.trace("[txn %s] END set_field" % writer)
            # increment the document count for this term
            t = [field.fieldname, field.fieldtype, term]
            try:
                logger.trace("[txn %s] BEGIN get_term" % writer)
                value = segment.get_term(writer._txn, t, RMW=True)
                logger.trace("[txn %s] END get_term" % writer)
                if not u'num-docs' in value:
                    raise WriterError("term %s is missing key 'num-docs'" % t)
                value[u'num-docs'] += 1
            except KeyError:
                value = {u'num-docs': 1}
            logger.trace("[txn %s] BEGIN set_term" % writer)
            segment.set_term(writer._txn, t, value)
            logger.trace("[txn %s] END set_term" % writer)
            # add the posting
            if posting == None:
                posting = dict()
            p = [field.fieldname, field.fieldtype, term, evid.ts, evid.offset]
            logger.trace("[txn %s] BEGIN set_posting" % writer)
            segment.set_posting(writer._txn, p, posting)
            logger.trace("[txn %s] END set_posting" % writer)
        return deferToThread(_newPosting, self, field, term, evid, posting)

    def commit(self):
        if self._txn == None:
            succeed(None)
        def _commit(writer):
            txn = writer._txn
            writer._txn = None
            if writer._numEvents > 0:
                ix = writer._ix
                logger.trace("[txn %s] waiting for writeLock" % writer)
                with writer._ix._writeLock:
                    logger.trace("[txn %s] acquired writeLock" % writer)
                    logger.trace("[txn %s] BEGIN commit" % writer)
                    txn.commit()
                    logger.trace("[txn %s] END commit" % writer)
                    ix._indexSize += writer._numEvents
                    ix._currentSize += writer._numEvents
                    if ix._lastId == None or writer._lastId > ix._lastId:
                        ix._lastId = writer._lastId
                    if ix._lastModified == None or writer._lastModified > ix._lastModified:
                        ix._lastModified = writer._lastModified
                logger.trace("[txn %s] released writeLock" % writer)
            else:
                txn.commit()
        return deferToThread(_commit, self)

    def abort(self):
        if self._txn == None:
            succeed(None)
        def _abort(writer):
            txn = writer._txn
            writer._txn = None
            logger.trace("[txn %s] BEGIN abort" % writer)
            txn.abort()
            logger.trace("[txn %s] END abort" % writer)
        return deferToThread(_abort, self)
