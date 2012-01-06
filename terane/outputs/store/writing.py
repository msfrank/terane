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

import pickle, time
from zope.interface import implements
from terane.bier.writing import IWriter, WriterError
from terane.outputs.store.encoding import json_encode, json_decode
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.writing')

class WriterExpired(WriterError):
    pass

class IndexWriter(object):

    implements(IWriter)

    def __init__(self, ix):
        self._ix = ix
        self._segment = ix._current[0]
        self._txn = None

    def __enter__(self):
        if self._txn:
            raise WriterError("IndexWriter is already in a transaction")
        self._txn = self._ix.new_txn()
        return self

    def newDocument(self, docId, document):
        self._segment.set_doc(self._txn, str(docId), json_encode(document))
        try:
            last_update = json_decode(self._segment.get_meta(self._txn, 'last-update'))
            if 'size' not in last_update:
                raise WriterError("segment metadata corruption: no such key 'size'")
            last_update['size'] += 1
        except KeyError:
            last_update = {'size': 1}
        last_update['last-id'] = str(docId)
        last_update['last-modified'] = int(time.time())
        self._segment.set_meta(self._txn, 'last-update', json_encode(last_update))

    def newPosting(self, fieldname, term, docId, value):
        try:
            tmeta = json_decode(self._segment.get_term_meta(self._txn, fieldname, term))
            if not 'num-docs' in tmeta:
                raise WriterError("term metadata corruption: no such key 'num-docs'")
            tmeta['num-docs'] += 1
        except KeyError:
            tmeta = {'num-docs': 1}
        # increment the document count for this term
        self._segment.set_term_meta(self._txn, fieldname, term, json_encode(tmeta))
        try:
            fmeta = json_decode(self._segment.get_field_meta(self._txn, fieldname))
            if not 'num-docs' in fmeta:
                raise WriterError("field metadata corruption: no such key 'num-docs'")
            fmeta['num-docs'] += 1
        except KeyError:
            fmeta = {'num-docs': 1}
        # increment the document count for this field
        self._segment.set_field_meta(self._txn, fieldname, json_encode(fmeta))
        if value == None:
            value = dict()
        # add the term to the reverse index
        self._segment.set_term(self._txn, fieldname, term, str(docId), json_encode(value))

    def __exit__(self, excType, excValue, traceback):
        if (excType, excValue, traceback) == (None, None, None):
            self._txn.commit()
        else:
            self._txn.abort()
        self._ix = None
        self._segment = None
        self._txn = None
        return False
