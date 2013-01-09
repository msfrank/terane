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
        # get the current schema
        schema = yield self.index.getSchema()
        if not ISchema.providedBy(schema):
            raise TypeError("index schema does not implement ISchema")
        # create a new transactional writer
        writer = yield self.index.newWriter()
        if not IWriter.providedBy(writer):
            raise TypeError("index writer does not implement IWriter")
        # update the index in the context of a writer
        yield writer.begin()
        try:   
            # create a new event identifier
            evid = EVID.fromEvent(self.event)
            # process the value of each field in the event
            fields = {}
            for fieldname, fieldtype, value in self.event:
                try:
                    field = yield schema.getField(fieldname, fieldtype)
                except KeyError:
                    field = yield schema.addField(fieldname, fieldtype)
                # update the field with the event value
                for term,meta in field.parseValue(value):
                    yield writer.newPosting(field, term, evid, meta)
                fields[fieldname] = value
            # store the document data
            yield writer.newEvent(evid, fields)
            # otherwise commit the transaction and return the event id
            yield writer.commit()
        # if an exception was raised, then abort the transaction
        except Exception, e:
            yield writer.abort()
            raise e
