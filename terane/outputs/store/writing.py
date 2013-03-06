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
import cPickle as pickle
from zope.interface import implements
from twisted.internet.defer import succeed
from twisted.internet.threads import deferToThread
from terane.bier import IWriter
from terane.bier.fields import QualifiedField
from terane.bier.writing import WriterError
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.writing')

class WriterExpired(WriterError):
    pass

class IndexWriter(object):

    implements(IWriter)

    def __init__(self, ix):
        self._ix = ix
        logger.trace("[writer %s] waiting for segmentLock" % self)
        with ix._segmentLock:
            logger.trace("[writer %s] acquired segmentLock" % self)
            self._segment = ix._current
        logger.trace("[writer %s] released segmentLock" % self)

    def __str__(self):
        return "%x" % id(self)

    def getField(self, fieldname, fieldtype):
        """
        Return the field specified by the fieldname and fieldtype.  If the
        field doesn't exist, create it.
        """
        def _getField(writer, fieldname, fieldtype):
            ix = writer._ix
            logger.trace("[writer %s] waiting for fieldLock" % self)
            with ix._fieldLock:
                logger.trace("[writer %s] acquired fieldLock" % self)
                if fieldname in ix._fields:
                    fieldspec = ix._fields[fieldname]
                else:
                    fieldspec = {}
                if not fieldtype in fieldspec:
                    field = ix._fieldstore.getField(fieldtype)
                    stored = QualifiedField(fieldname, fieldtype, field)
                    fieldspec[fieldtype] = stored
                    pickled = unicode(pickle.dumps(fieldspec))
                    with ix.new_txn() as txn:
                        logger.trace("[txn %x] BEGIN set_field" % txn.id())
                        ix.set_field(txn, fieldname, pickled, NOOVERWRITE=True)
                        logger.trace("[txn %x] END set_field" % txn.id())
                    ix._fields[fieldname] = fieldspec
            logger.trace("[writer %s] released fieldLock" % self)
            return fieldspec[fieldtype]
        return deferToThread(_getField, self, fieldname, fieldtype)

    def newEvent(self, evid, event):
        def _newEvent(writer, evid, event):
            ix = writer._ix
            segment = writer._segment
            with ix.new_txn() as txn:
                # serialize the fields dict and write it to the segment
                logger.trace("[txn %x] BEGIN set_event" % txn.id())
                segment.set_event(txn, [evid.ts,evid.offset], event,
                                  NOOVERWRITE=True)
                logger.trace("[txn %x] END set_event" % txn.id())
            lastModified = int(time.time())
            # update segment metadata
            with ix.new_txn() as txn:
                try:
                    logger.trace("[txn %x] BEGIN get_meta" % txn.id())
                    lastUpdate = segment.get_meta(txn, u'last-update', RMW=True)
                    logger.trace("[txn %x] END get_meta" % txn.id())
                except KeyError:
                    lastUpdate = {
                        u'segment-size': 0,
                        u'last-id': [evid.ts, evid.offset],
                        u'last-modified': lastModified
                        }
                assert(u'segment-size' in lastUpdate)
                assert(u'last-id' in lastUpdate)
                assert(u'last-modified' in lastUpdate)
                lastUpdate[u'segment-size'] = lastUpdate[u'segment-size'] + 1 
                lastUpdate[u'last-id'] = [evid.ts, evid.offset]
                lastUpdate[u'last-modified'] = lastModified
                logger.trace("[txn %x] BEGIN set_meta" % txn.id())
                segment.set_meta(txn, u'last-update', lastUpdate)
                logger.trace("[txn %x] END set_meta" % txn.id())
            # update index metadata
            with ix.new_txn() as txn:
                try:
                    logger.trace("[txn %x] BEGIN get_meta" % txn.id())
                    lastUpdate = ix.get_meta(txn, u'last-update', RMW=True)
                    logger.trace("[txn %x] END get_meta" % txn.id())
                except KeyError:
                    lastUpdate = {
                        u'index-size': 0,
                        u'last-id': [evid.ts, evid.offset],
                        u'last-modified': lastModified
                        }
                assert(u'index-size' in lastUpdate)
                assert(u'last-id' in lastUpdate)
                assert(u'last-modified' in lastUpdate)
                lastUpdate[u'index-size'] = lastUpdate[u'index-size'] + 1 
                lastUpdate[u'last-id'] = [evid.ts, evid.offset]
                lastUpdate[u'last-modified'] = lastModified
                logger.trace("[txn %x] BEGIN set_meta" % txn.id())
                ix.set_meta(txn, u'last-update', lastUpdate)
                logger.trace("[txn %x] END set_meta" % txn.id())
        return deferToThread(_newEvent, self, evid, event)

    def newPosting(self, field, term, evid, posting):
        def _newPosting(writer, field, term, evid, posting):
            ix = writer._ix
            segment = writer._segment
            with ix.new_txn() as txn:
                # increment the document count for this field
                f = [field.fieldname, field.fieldtype]
                try:
                    logger.trace("[txn %x] BEGIN get_field" % txn.id())
                    value = segment.get_field(txn, f, RMW=True)
                    logger.trace("[txn %x] END get_field" % txn.id())
                except KeyError:
                    value = {u'num-docs': 0}
                assert(u'num-docs' in value)
                value[u'num-docs'] = value[u'num-docs'] + 1
                logger.trace("[txn %x] BEGIN set_field" % txn.id())
                segment.set_field(txn, f, value)
                logger.trace("[txn %x] END set_field" % txn.id())
                # increment the document count for this term
                t = [field.fieldname, field.fieldtype, term]
                try:
                    logger.trace("[txn %x] BEGIN get_term" % txn.id())
                    value = segment.get_term(txn, t, RMW=True)
                    logger.trace("[txn %x] END get_term" % txn.id())
                except KeyError:
                    value = {u'num-docs': 1}
                assert(u'num-docs' in value)
                value[u'num-docs'] = value[u'num-docs'] + 1
                logger.trace("[txn %x] BEGIN set_term" % txn.id())
                segment.set_term(txn, t, value)
                logger.trace("[txn %x] END set_term" % txn.id())
                # add the posting
                posting = dict() if posting == None else posting
                p = [field.fieldname, field.fieldtype, term, evid.ts, evid.offset]
                logger.trace("[txn %x] BEGIN set_posting" % txn.id())
                segment.set_posting(txn, p, posting)
                logger.trace("[txn %x] END set_posting" % txn.id())
        return deferToThread(_newPosting, self, field, term, evid, posting)

    def close(self):
        return succeed(None)
