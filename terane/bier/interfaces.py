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

from zope.interface import Interface
from terane.plugins import ILoadable

class IField(ILoadable):
    def validateValue(self, value):
        """
        Verify that the supplied value is valid.

        :param value: The value to validate.
        :type value: object
        :returns: The value.
        :rtype: object
        :raises TypeError: The value is invalid.
        """
    def parseValue(self, value):
        """
        Return a list of tuples, each containing a tokenized term and a dict
        containing term metadata.

        :param value: The value to parse.
        :type value: object
        :returns: A list of (term, metadata) tuples.
        :rtype: list
        """
    def makeMatcher(self, fieldname, funcname, value):
        """
        Build a matcher from the supplied value.

        :param value: The value to tokenize.
        :type value: object
        :returns: The matcher.
        :rtype: An object implementing :class:`terane.bier.IMatcher`
        """

class ISchema(Interface):
    def hasField(fieldname, fieldtype):
        """
        Determine whether a field exists in the schema.

        :param fieldname: The name of the field.
        :type fieldname: str
        :param fieldtype: The type of the field.
        :type fieldtype: str
        :returns: True if the field exists in the schema, otherwise False.
        :rtype: bool
        """
    def getField(fieldname, fieldtype):
        """
        Returns the specified Field.

        :param fieldname: The name of the field.
        :type fieldname: str
        :param fieldtype: The type of the field.
        :type fieldtype: str
        :returns: A qualified field.
        :rtype: :class:`terane.bier.field.QualifiedField`
        :raises KeyError: The field does not exist in the schema.
        """
    def addField(fieldname, fieldtype):
        """
        Adds a new field to the schema.

        :param fieldname: The name of the field.
        :type fieldname: str
        :param fieldtype: The type of the field.
        :type fieldtype: str
        :returns: A qualified field.
        :rtype: :class:`terane.bier.field.QualifiedField`
        :raises KeyError: The field already exists in the schema.
        """
    def listFields():
        """
        Returns a list of fields present in the schema.

        :returns: The list of qualified fields.
        :rtype: list
        """

class IPostingList(Interface):
    def nextPosting():
        """
        Returns the next posting, or None if iteration is finished.

        :returns: A tuple containing the evid, the term value, and the store, or (None,None,None)
        :rtype: tuple
        """
    def skipPosting(targetId):
        """
        Skips to the targetId, returning the posting or None if the posting doesn't exist.

        :param targetId: The target evid to skip to.
        :type targetId: :class:`terane.bier.evid.EVID`
        :returns: A tuple containing the evid, the term value, and the store, or (None,None,None)
        :rtype: tuple
        """
    def close():
        """
        Frees any resources associated with the searcher.
        """

class IMatcher(Interface):
    def optimizeMatcher(index):
        """
        Optimizes the matcher.  This may return the object itself, a new object, or
        None if the query completely optimizes away.

        :param index: The index we are querying.
        :type index: An object implementing :class:`terane.bier.index.IIndex`
        :returns: An optimized matcher.
        :rtype: An object implementing :class:`terane.bier.searching.IMatcher`
        """
    def matchesLength(searcher, startId, endId):
        """
        Returns an estimate of the number of matching postings within the specified period.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: An estimate of the number of postings.
        :rtype: int
        """
    def iterMatches(searcher, startId, endId):
        """
        Returns an object implementing IPostingList which yields each matching posting
        within the specified period.
        
        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :param reverse: If True, then reverse the order of iteration.
        :type reverse: bool
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.searching.IPostingList`
        """

class ISearcher(Interface):
    def postingsLength(field, term, startId, endId):
        """
        Returns an estimate of the number of possible postings for the term in the
        specified field.  As a special case, if fieldname and term are None, then
        return an estimate of the number of documents within the specified period.

        :param field: The field to search within.
        :type field: :class:`terane.bier.fields.QualifiedField`
        :param term: The term to search for.
        :type term: object
        :param startId:
        :type startId: :class:`terane.bier.evid.EVID`
        :param endId:
        :type endId: :class:`terane.bier.evid.EVID`
        :returns: An estimate of the number of postings.
        :rtype: int
        """
    def iterPostings(field, term, startId, endId):
        """
        Returns an object implementing IPostingList which yields postings for the
        term in the specified field.  As a special case, if fieldname and term are
        None, then yield postings for all terms in all fields within the specified
        period.

        :param field: The field to search within.
        :type field: :class:`terane.bier.fields.QualifiedField`
        :param term: The term to search for.
        :type term: object
        :param startId:
        :type startId: :class:`terane.bier.evid.EVID`
        :param endId:
        :type endId: :class:`terane.bier.evid.EVID`
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.searching.IPostingList`
        """
    def close():
        """
        Frees any resources associated with the searcher.
        """

class IEventStore(Interface):
    def getEvent(evid):
        """
        Returns the event specified by evid.

        :param evid: The event identifier.
        :type evid: :class:`terane.bier.evid.EVID`
        :returns: A dict mapping qualified fields to values.
        :rtype: dict
        """

class IWriter(Interface):
    def begin():
        """
        Enter the transactional context.
        """
    def newEvent(fields, evid):
        """
        Create a new event with the specified event identifier.

        :param fields: A map of fieldnames to values.
        :type fields: dict
        :param evid: The identifier used as key to look up the event.
        :type evid: :class:`terane.bier.evid.EVID`
        """
    def newPosting(field, term, evid, meta):
        """
        Create a new posting for the field term with the specified event identifier.

        :param field: The field the term is associated with.
        :type field: :class:`terane.bier.field.QualifiedField`
        :param term: The term to be indexed.
        :type term: object
        :param evid: The identifier used as key to look up the event.
        :type evid: :class:`terane.bier.evid.EVID`
        :param meta: Metadata associated with the posting.
        :type meta: dict
        """
    def commit():
        """
        Exit the transactional context, committing any modifications.
        """
    def abort():
        """
        Exit the transactional context, discarding any modifications.
        """
          
class IIndex(Interface):
    def getSchema():
        """
        Returns an object implementing ISchema.
        """
    def newSearcher():
        """
        Returns an object implementing ISearcher.
        """
    def newWriter():
        """
        Returns an object implementing IWriter.
        """
    def getStats():
        """
        Returns a dict with Index statistics.
        """

class IEventFactory(Interface):
    def makeEvent():
        """
        Return a new Event with ts and id set, and message, hostname, and input
        fields set to empty values.

        :returns: A new Event.
        :rtype: :class:`terane.bier.event.Event`
        """

class IFieldStore(Interface):
    def getField(name):
        """
        Returns a field object corresponding to the specified name.

        :param name: The field name.
        :type name: string
        :returns: The field object.
        :rtype: An object providing :class:`terane.bier.interfaces.IField`
        """
