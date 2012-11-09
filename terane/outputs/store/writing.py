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
from terane.bier import IWriter
from terane.bier.writing import WriterError
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.writing')

class WriterExpired(WriterError):
    pass

class IndexWriter(object):

    implements(IWriter)

    def __init__(self, ix):
        self._txn = None
        self._ix = ix
        self._indexSize = self._ix._indexSize
        self._currentSize = self._ix._currentSize
        self._lastId = self._ix._lastId
        self._lastModified = self._ix._lastModified

    def begin(self):
        if self._txn:
            raise WriterError("IndexWriter is already in a transaction")
        self._txn = self._ix.new_txn()
        return self

    def newEvent(self, evid, event):
        # serialize the fields dict and write it to the segment
        segment = self._ix._current
        segment.set_event(self._txn, str(evid), event)
        # update segment metadata
        self._indexSize += 1
        self._currentSize += 1
        self._lastId = str(evid)
        self._lastModified = int(time.time())
        lastUpdate = {
            u'size': self._currentSize,
            u'last-id': self._lastId,
            u'last-modified': self._lastModified
            }
        segment.set_meta(self._txn, u'last-update', lastUpdate)

    def newPosting(self, field, term, evid, posting):
        segment = self._ix._current
        fieldspec = (field.fieldname,field.fieldtype)
        try:
            value = segment.get_term(self._txn, fieldspec, term)
            if not u'num-docs' in value:
                raise WriterError("term metadata corruption: no such key 'num-docs'")
            value[u'num-docs'] += 1
        except KeyError:
            value = {u'num-docs': 1}
        # increment the document count for this term
        segment.set_term(self._txn, fieldspec, term, value)
        try:
            value = segment.get_field(self._txn, fieldspec)
            if not u'num-docs' in value:
                raise WriterError("field metadata corruption: no such key 'num-docs'")
            value[u'num-docs'] += 1
        except KeyError:
            value = {u'num-docs': 1}
        # increment the document count for this field
        segment.set_field(self._txn, fieldspec, value)
        if posting == None:
            posting = dict()
        # add the term to the reverse index
        segment.set_posting(self._txn, fieldspec, term, str(evid), posting)

    def commit(self):
        self._txn.commit()
        self._ix._indexSize = self._indexSize
        self._ix._currentSize = self._currentSize
        self._ix._lastId = self._lastId
        self._ix._lastModified = self._lastModified
        self._txn = None

    def abort(self):
        self._txn.abort()
        self._txn = None
