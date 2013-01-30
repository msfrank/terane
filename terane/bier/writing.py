# Copyright 2010,2011,2012 Michael Frank <msfrank@syntaxjockey.com>
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

from twisted.python.failure import Failure
from terane.bier.interfaces import IIndex, ISchema, IWriter
from terane.bier.evid import EVID
from terane.bier.event import Event
from terane.loggers import getLogger

logger = getLogger('terane.bier.writing')

class WriterError(Exception):
    pass

class WriterWorker(object):
    """
    A worker which writes an event to the specified index.  Instances of this
    class must be submitted to a :class:`terane.sched.Task` to be scheduled.
    """

    def __init__(self, event, index):
        """
        :param event: The event to write.
        :type event: :class:`terane.bier.event.Event`
        :param index: The index which will receive the event.
        :type index: Object implementing :class:`terane.bier.interfaces.IIndex`
        """
        if not isinstance(event, Event):
            raise TypeError("event is not an Event")
        self.event = event
        # verify that the index provides the appropriate interface
        if not IIndex.providedBy(index):
            raise TypeError("index does not implement IIndex")
        self.index = index


    def next(self):
        w = str(hash(self))
        evid = EVID.fromEvent(self.event)
        writer = None
        try:
            # store the event
            writer = yield self.index.newWriter()
            if not IWriter.providedBy(writer):
                raise TypeError("index writer does not implement IWriter")
            fields = dict([(fn,v) for fn,ft,v in self.event])
            logger.trace("[writer %s] creating event %s" % (w,evid))
            yield writer.newEvent(evid, fields)
            yield writer.commit()
            writer = None
            # process the value of each field in the event
            for fieldname, fieldtype, value in self.event:
                writer = yield self.index.newWriter()
                if not IWriter.providedBy(writer):
                    raise TypeError("index writer does not implement IWriter")
                schema = yield writer.getSchema()
                if not ISchema.providedBy(schema):
                    raise TypeError("index schema does not implement ISchema")
                try:
                    logger.trace("[writer %s] retrieving field %s:%s" % (w,fieldname,fieldtype))
                    field = yield schema.getField(fieldname, fieldtype)
                except KeyError:
                    logger.trace("[writer %s] adding field %s:%s" % (w,fieldname,fieldtype))
                    field = yield schema.addField(fieldname, fieldtype)
                # store a posting for each term in each field
                for term,meta in field.parseValue(value):
                    logger.trace("[writer %s] creating posting %s:%s:%s" % (w,field,term,evid))
                    yield writer.newPosting(field, term, evid, meta)
                yield writer.commit()
                writer = None
        except:
            # if an exception was raised and a transaction is open, abort it
            if writer != None:
                logger.debug("[writer %s] aborting txn" % w)
                yield writer.abort()
            raise
        # otherwise commit the transaction
        logger.debug("[writer %s] committed event %s" % (w,str(evid)))
