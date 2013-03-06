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
from terane.bier.interfaces import IIndex, IWriter
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

    def __str__(self):
        return "%x" % id(self)

    def next(self):
        writer = None
        evid = EVID.fromEvent(self.event)
        try:
            # store the event
            writer = yield self.index.newWriter()
            if not IWriter.providedBy(writer):
                raise TypeError("index writer does not implement IWriter")
            fields = dict([(fn,v) for fn,ft,v in self.event])
            logger.trace("[writer %s] creating event %s" % (self,evid))
            yield writer.newEvent(evid, fields)
            # process the value of each field in the event
            for fieldname, fieldtype, value in self.event:
                logger.trace("[writer %s] using field %s:%s" % (self,fieldname,fieldtype))
                field = yield writer.getField(fieldname, fieldtype)
                # store a posting for each term in each field
                for term,meta in field.parseValue(value):
                    logger.trace("[writer %s] creating posting %s:%s:%s" % (self,field,term,evid))
                    yield writer.newPosting(field, term, evid, meta)
            logger.debug("[writer %s] committed event %s" % (self,str(evid)))
        finally:
            if writer != None:
                yield writer.close()
