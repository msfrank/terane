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

class ISchema(Interface):
    def hasField(name):
        """
        Determine whether a field exists in the schema.

        :param name: The name of the field.
        :type name: str
        :returns True if the field exists in the schema, otherwise False.
        :rtype: bool
        """
    def getField(name):
        """
        Returns the specified Field.

        :param name: The name of the field.
        :type name: str
        :returns: The field specified by name.
        :rtype: A subclass of :class:`terane.bier.schema.BaseField`
        """
    def addField(name, field):
        """
        Adds a new field to the schema.

        :param name: The name of the field.
        :type name: str
        :param field: The field to add.
        :type field: A subclass of :class:`terane.bier.schema.BaseField`
        """
    def listFields():
        """
        Returns a list of field names present in the schema.

        :returns: The list of field names.
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
    def postingsLength(fieldname, term, startId, endId):
        """
        Returns an estimate of the number of possible postings for the term in the
        specified field.  As a special case, if fieldname and term are None, then
        return an estimate of the number of documents within the specified period.

        :param fieldname: The name of the field to search within.
        :type fieldname: str
        :param term: The term to search for.
        :type term: unicode
        :param startId:
        :type startId: :class:`terane.bier.evid.EVID`
        :param endId:
        :type endId: :class:`terane.bier.evid.EVID`
        :returns: An estimate of the number of postings.
        :rtype: int
        """
    def iterPostings(fieldname, term, startId, endId):
        """
        Returns an object implementing IPostingList which yields postings for the
        term in the specified field.  As a special case, if fieldname and term are
        None, then yield postings for all terms in all fields within the specified
        period.

        :param fieldname: The name of the field to search within.
        :type fieldname: str
        :param term: The term to search for.
        :type term: unicode
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
        :returns: A dict mapping fieldnames to values.
        :rtype: dict
        """

class IWriter(Interface):
    def begin():
        """
        Enter the transactional context.
        """
    def newEvent(evid, fields):
        """
        Create a new event with the specified event identifier.

        :param evid: The event identifier to use for the document."
        :type evid: :class:`terane.bier.evid.EVID`
        :param fields: The event fields.
        :type fields: dict
        """
    def newPosting(fieldname, term, evid, value):
        """
        Create a new posting for the field term with the specified event identifier.

        :param fieldname:
        :type fieldname: str
        :param term:
        :type term: unicode
        :param evid:
        :type evid: :class:`terane.bier.evid.EVID`
        :param value:
        :type value:
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
    def schema():
        """
        Returns an object implementing ISchema.
        """
    def searcher():
        """
        Returns an object implementing ISearcher.
        """
    def writer():
        """
        Returns an object implementing IWriter.
        """
    def getStats():
        """
        Returns a dict with Index statistics.
        """
