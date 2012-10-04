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

from terane.bier.interfaces import IIndex, ISchema, IWriter
from terane.bier.evid import EVID
from terane.loggers import getLogger

logger = getLogger('terane.bier.writing')

class WriterError(Exception):
    pass

def writeEventToIndex(event, index):
    """

    :param event:
    :type event:
    :param index:
    :type index:
    :returns: The EVID of the new event.
    :rtype: :class:`terane.bier.evid.EVID`
    """
    # verify that the index provides the appropriate interface
    if not IIndex.providedBy(index):
        raise TypeError("index does not implement IIndex")
    # get the current schema
    schema = index.getSchema()
    if not ISchema.providedBy(schema):
        raise TypeError("index schema does not implement ISchema")

    # create a new transactional writer
    writer = index.newWriter()
    if not IWriter.providedBy(writer):
        raise TypeError("index writer does not implement IWriter")
    writer.begin()

    # update the index in the context of a writer
    try:   
        # create a new event identifier
        evid = EVID.fromEvent(event)
        # process the value of each field in the event
        fields = {}
        for fieldname,fieldtype,value in event.fields():
            if not schema.hasField(fieldname, fieldtype):
                schema.addField(fieldname, fieldtype)
            field = schema.getField(fieldname, fieldtype)
            # update the field with the event value
            for term,meta in field.parseValue(value):
                writer.newPosting(field, term, evid, meta)
            fields[field] = value
        # store the document data
        logger.trace("id=%s: fields=%s" % (evid, fields))
        writer.newEvent(evid, fields)
    # if an exception was raised, then abort the transaction
    except:
        writer.abort()
        raise
    # otherwise commit the transaction and return the event id
    writer.commit()
    return evid
