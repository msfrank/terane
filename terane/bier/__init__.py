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
        """
    def addField(name, field):
        """
        Adds a new field to the schema.
        """
    def listFields():
        """
        """

class IPostingList(Interface):
    def nextPosting():
        """
        Returns the next posting, or None if iteration is finished.

        :returns: A tuple containing the docId, the term value, and the store, or (None,None,None)
        :rtype: tuple
        """
    def skipPosting(targetId):
        """
        Skips to the targetId, returning the posting or None if the posting doesn't exist.

        :param targetId: The target docId to skip to.
        :type targetId: :class:`terane.bier.docid.DocID`
        :returns: A tuple containing the docId, the term value, and the store, or (None,None,None)
        :rtype: tuple
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
        Returns an estimate of the  number of possible postings for the term
        in the specified field.

        :param fieldname: The name of the field to search within.
        :type fieldname: str
        :param term: The term to search for.
        :type term: unicode
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: An estimate of the number of postings.
        :rtype: int
        """
    def iterPostings(fieldname, term, startId, endId):
        """
        Returns an object implementing IPostingList which yields postings for the
        term in the specified field.

        :param fieldname: The name of the field to search within.
        :type fieldname: str
        :param term: The term to search for.
        :type term: unicode
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :param reverse: If True, then reverse the order of iteration.
        :type reverse: bool
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.searching.IPostingList`
        """

class IEventStore(Interface):
    def getEvent(docId):
        """
        Returns the event specified by docId.

        :param docId: The event docId
        :type docId: :class:`terane.bier.docid.DocID`
        :returns: A dict mapping fieldnames to values.
        :rtype: dict
        """

class IWriter(Interface):
    def __enter__():
        "Enter the transactional context."
    def newDocument(docId, document):
        "Create a new document with the specified document ID."
    def newPosting(fieldname, term, docId, value):
        "Create a new posting for the field term with the specified document ID."
    def __exit__(excType, excValue, traceback):
        "Exit the transactional context."
          
class IIndex(Interface):
    def schema():
        "Returns an object implementing ISchema."
    def searcher():
        "Returns an object implementing ISearcher."
    def writer():
        "Returns an object implementing IWriter."
    def newDocumentId(ts):
        "Returns a new document ID."
