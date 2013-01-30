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
        with ix._writeLock:
            self._segment = ix._current
        self._ix = ix
        self._txn = ix.new_txn()
        self._numEvents = 0
        self._lastId = None
        self._lastModified = None

    def getSchema(self):
        return succeed(Schema(self._ix, self._txn))

    def newEvent(self, evid, event):
        def _newEvent(writer, evid, event):
            segment = writer._segment
            # serialize the fields dict and write it to the segment
            segment.set_event(writer._txn, [evid.ts,evid.offset], event,
                              NOOVERWRITE=True)
            # update segment metadata
            writer._numEvents += 1
            writer._lastId = evid
            writer._lastModified = int(time.time())
            lastUpdate = {
                u'size': writer._ix._currentSize + 1,
                u'last-id': [evid.ts, evid.offset],
                u'last-modified': writer._lastModified
                }
            segment.set_meta(writer._txn, u'last-update', lastUpdate)
        return deferToThread(_newEvent, self, evid, event)

    def newPosting(self, field, term, evid, posting):
        def _newPosting(writer, field, term, evid, posting):
            segment = writer._segment
            # increment the document count for this field
            f = [field.fieldname, field.fieldtype]
            try:
                value = segment.get_field(writer._txn, f, RMW=True)
                if not u'num-docs' in value:
                    raise WriterError("field %s is missing key 'num-docs'" % f)
                value[u'num-docs'] += 1
            except KeyError:
                value = {u'num-docs': 1}
            segment.set_field(writer._txn, f, value)
            # increment the document count for this term
            t = [field.fieldname, field.fieldtype, term]
            try:
                value = segment.get_term(writer._txn, t, RMW=True)
                if not u'num-docs' in value:
                    raise WriterError("term %s is missing key 'num-docs'" % t)
                value[u'num-docs'] += 1
            except KeyError:
                value = {u'num-docs': 1}
            segment.set_term(writer._txn, t, value)
            # add the posting
            if posting == None:
                posting = dict()
            p = [field.fieldname, field.fieldtype, term, evid.ts, evid.offset]
            segment.set_posting(writer._txn, p, posting)
        return deferToThread(_newPosting, self, field, term, evid, posting)

    def commit(self):
        if self._txn == None:
            succeed(None)
        def _commit(writer):
            txn = writer._txn
            writer._txn = None
            if writer._numEvents > 0:
                ix = writer._ix
                with writer._ix._writeLock:
                    txn.commit()
                    ix._indexSize += writer._numEvents
                    ix._currentSize += writer._numEvents
                    if ix._lastId == None or writer._lastId > ix._lastId:
                        ix._lastId = writer._lastId
                    if ix._lastModified == None or writer._lastModified > ix._lastModified:
                        ix._lastModified = writer._lastModified
            else:
                txn.commit()
        return deferToThread(_commit, self)

    def abort(self):
        if self._txn == None:
            succeed(None)
        def _abort(writer):
            txn = writer._txn
            writer._txn = None
            txn.abort()
        return deferToThread(_abort, self)
