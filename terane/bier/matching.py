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

import time, datetime, copy, bisect
from zope.interface import implements
from terane.bier import IMatcher, IPostingList
from terane.bier.docid import DocID
from terane.loggers import getLogger

logger = getLogger('terane.bier.matching')

class Term(object):
    """
    The basic query.  In order for an event to match, the term must be present in the
    specified field.
    """

    implements(IMatcher, IPostingList)

    def __init__(self, fieldname, value):
        """
        :param fieldname: The name of the field to search.
        :type fieldname: str
        :param value: The term to search for in the field.
        :type value: unicode
        """
        if fieldname == None:
            self.fieldname = 'default'
        else:
            self.fieldname = fieldname
        self.value = unicode(value)

    def __str__(self):
        return "<Term %s=%s>" % (self.fieldname,self.value)

    def optimizeMatcher(self, index):
        """
        Optimize the matcher.  If the field does not exist in the index, then toss the
        matcher.  If the normalized term value splits into multiple terms, then return
        the union of the terms.

        :param index: The index we will be running the query on.
        :type index: Object implementing :class:`terane.bier.IIndex`
        :returns: The optimized matcher.
        :rtype: An object implementing :class:`terane.bier.IMatcher`
        """
        schema = index.schema()
        if not schema.hasField(self.fieldname):
            return None
        field = schema.getField(self.fieldname)
        terms = list([term for term,_ in field.terms(self.value)])
        if len(terms) == 0:
            return None
        elif len(terms) > 1:
            return OR([Term(self.fieldname,t).optimizeMatcher(index) for t in terms])
        self.value = terms[0]
        return self

    def matchesLength(self, searcher, startId, endId):
        """
        Returns an estimate of the approximate number of matching postings which will
        be returned using the specified searcher within the specified period.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: The postings length estimate.
        :rtype: int
        """
        length = searcher.postingsLength(self.fieldname, self.value, startId, endId)
        logger.trace("%s: postingsLength() => %i" % (self, length))
        return length

    def iterMatches(self, searcher, startId, endId):
        """
        Returns an object for iterating through matching postings.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
          
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.IPostingList`
        """
        self._postings = searcher.iterPostings(self.fieldname, self.value, startId, endId)
        return self

    def nextPosting(self):
        """
        Returns the next matching posting, or (None,None,None) if there are no
        more matching postings.

        :returns: The posting of the next matching event, or (None,None,None).
        :rtype: tuple
        """
        posting = self._postings.nextPosting()
        logger.trace("%s: nextPosting() => %s" % (self, posting[0])) 
        return posting

    def skipPosting(self, targetId):
        """
        Returns the posting matching targetId if the matcher contains the specified targetId,
        or None if the targetId is not present.

        :param targetId: The target docId.
        :type targetId: :class:`terane.bier.docid.DocID`
        :returns: The posting matching the targetId, or (None,None,None).
        :rtype: tuple
        """
        posting = self._postings.skipPosting(targetId)
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, posting[0]))
        return posting

class AND(object):
    """
    The AND operator is an intersection matcher.  In order for an event to match, it must
    match all child matchers.
    """

    implements(IMatcher, IPostingList)

    def __init__(self, children):
        """
        :param children: A list of child matchers which are to be intersected.
        :type children: list
        """
        self.children = children
        self._lengths = None

    def __str__(self):
        return "<AND [%s]>" % ', '.join([str(child) for child in self.children])

    def optimizeMatcher(self, index):
        """
        Optimize the matcher.  If any child matchers are AND operators, then move their
        children into this matcher.  If any child matchers optimize out, then toss them.
        If all child matchers optimize out, then we can toss the parent matcher as well.

        :param index: The index we will be running the query on.
        :type index: Object implementing :class:`terane.bier.IIndex`
        :returns: The optimized matcher.
        :rtype: An object implementing :class:`terane.bier.IMatcher`
        """
        children = []
        excludes = [] 
        for child in self.children:
            # optimize each child query
            child = child.optimizeMatcher(index)
            # if the child is an AND operator, then move all of the child's children
            # into this instance.
            if isinstance(child, AND):
                for subchild in child.children: children.append(subchild)
            # if the child is a NOT operator, then move it to the excludes list
            elif isinstance(child, NOT):
                excludes.append(child)
            # if the child has been optimized away, then toss it
            elif child != None:
                children.append(child)
        self.children = children
        # if there are no children, then we can toss this matcher too
        if len(self.children) == 0:
            return None
        # if there are any NOT operators, then we wrap this matcher and the combined NOTs in a Sieve.
        if len(excludes) > 0:
            return Sieve(self, excludes)
        return self

    def matchesLength(self, searcher, startId, endId):
        """
        Returns an estimate of the approximate number of postings which will be returned
        for the query using the specified searcher within the specified period.  The estimate
        for an AND operator is the minimum estimate of its child queries.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: The postings length estimate.
        :rtype: int
        """
        self._lengths = []
        for child in self.children:
            bisect.insort_right(self._lengths, (child.matchesLength(searcher, startId, endId),child))
        length = self._lengths[0][0]
        logger.trace("%s: matchesLength() => %i" % (self, length))
        return length

    def iterMatches(self, searcher, startId, endId):
        """
        Returns an object for iterating through events matching the query.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.searching.IPostingList`
        """
        if self._lengths == None:
            self.matchesLength(searcher, startId, endId)
        self._smallest = self._lengths[0][1].iterMatches(searcher, startId, endId)
        self._others = [child[1].iterMatches(searcher, startId, endId) for child in self._lengths[1:]]
        return self

    def nextPosting(self):
        """
        Returns the docId of the next event matching the query, or None if there are no
        more events which match the query.

        :returns: The docId of the next matching event, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        while True:
            posting = self._smallest.nextPosting()
            if posting[0] == None:
                break
            for child in self._others:
                if child.skipPosting(posting[0])[0] == None:
                    posting = (None,None,None)
                    break
            if posting[0] != None:
                break
        logger.trace("%s: nextPosting() => %s" % (self, posting[0])) 
        return posting

    def skipPosting(self, targetId):
        """
        Returns the docId matching targetId if the query contains the specified targetId,
        or None if the targetId is not present.

        :returns: The docId matching the targetId, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        posting = self._smallest.skipPosting(targetId)
        if posting[0] != None:
            for child in self._others:
                posting = child.skipPosting(targetId)
                if posting[0] == None:
                    break
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, posting[0]))
        return posting[0]

class OR(object):
    """
    The OR operator is a union query.  In order for an event to match, it must
    match at least one of the child queries.
    """

    implements(IMatcher, IPostingList)

    def __init__(self, children):
        """
        :param children: A list of child queries which are to be unioned.
        :type children: list
        """
        self.children = children

    def __str__(self):
        return "<OR [%s]>" % ', '.join([str(child) for child in self.children])

    def optimizeMatcher(self, index):
        """
        Optimize the query.  If any child queries are OR operators, then move their
        children into this query.  If any child queries optimize out, then toss them.
        If all child queries optimize out, then we can toss the parent query as well.

        :param index: The index we will be running the query on.
        :type index: Object implementing :class:`terane.bier.index.IIndex`
        :returns: The optimized query.
        :rtype: An object implementing :class:`terane.bier.searching.IQuery`
        """
        children = []
        excludes = []
        for child in self.children:
            # optimize each child matcher
            child = child.optimizeMatcher(index)
            # if the child is an OR operator, then move all of the child's children
            # into this instance.
            if isinstance(child, OR):
                for subchild in child.children: children.append(subchild)
            # if the child is a NOT operator, then move it to the excludes list
            elif isinstance(child, NOT):
                excludes.append(child)
            # if the child has been optimized away, then toss it
            elif child != None:
                children.append(child)
        self.children = children
        # if there are no children, then we can toss this matcher too
        if len(self.children) == 0:
            return None
        # if there are any NOT operators, then we wrap this matcher and the combined NOTs in a Sieve.
        if len(excludes) > 0:
            return Sieve(self, excludes)
        return self

    def matchesLength(self, searcher, startId, endId):
        """
        Returns an estimate of the approximate number of postings which will be returned
        for the query using the specified searcher within the specified period.  The estimate
        for an OR operator is the sum of the estimates of its child queries.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: The postings length estimate.
        :rtype: int
        """
        length = 0
        # the posting length estimate is the sum of all child queries
        for child in self.children:
            length += child.matchesLength(searcher, startId, endId)
        return length

    def iterMatches(self, searcher, startId, endId):
        """
        Returns an object for iterating through events matching the query.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.searching.IPostingList`
        """
        # smallestPostings contains the smallest posting for each child query, or (None,None,None) 
        self._smallestPostings = [(None,None,None) for i in range(len(self.children))]
        # iters contains the iterator (object implementing IPostingList) for each child query
        self._iters = [child.iterMatches(searcher, startid, endId) for child in self.children]
        # lastId is the last docId returned by the iterator
        self._lastId = None
        # set our cmp function, which determines the next docId to return.  if we are searching in
        # reverse order, then reverse the meaning if the regular comparison function.
        if endId < startId:
            self._cmp = lambda d1,d2: cmp(d2,d1)
        # otherwise use the normal comparison function
        else:
            self._cmp = cmp
        return self

    def nextPosting(self):
        """
        Returns the docId of the next event matching the query, or None if there are no
        more events which match the query.

        :returns: The docId of the next matching event, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        curr = 0
        # check each child iter for the lowest docId
        for i in range(len(self.children)):
            # if None, then get the next posting from the iter
            if self._smallestPostings[i][0] == None:
                self._smallestPostings[i] = self._iters[i].nextPosting()
            # if the posting docId is None, then check the next iter
            if self._smallestPostings[i][0] == None:
                continue
            # if the docId equals the last docId returned, then ignore it
            if self._lastId != None and self._smallestPostings[i][0] == self._lastId:
                self._smallestPostings[i] = (None,None,None)
                continue
            # we don't compare the first docId with itself
            if i == 0:
                continue
            # if the docId is the current smallest docId, then remember it
            if self._smallestPostings[curr][0] == None or \
              self._cmp(self._smallestPostings[i][0], self._smallestPostings[curr][0]) < 0:
                curr = i
        # update lastId with the docId
        posting = self._smallestPostings[curr]
        self._lastId = posting[0]
        # forget the docId so we don't return it again
        self._smallestPostings[curr] = (None,None,None)
        logger.trace("%s: nextPosting() => %s" % (self, posting[0])) 
        return posting

    def skipPosting(self, targetId):
        """
        Returns the docId matching targetId if the query contains the specified targetId,
        or None if the targetId is not present.

        :returns: The docId matching the targetId, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        posting = None
        # iterate through each child query
        for i in range(len(self.children)):
            posting = self._smallestPostings[i]
            # if the smallestId equals the targetId, we are done
            if posting[0] == targetId:
                break
            # otherwise check if the targetId exists in the child query
            if posting[0] == None or self._cmp(posting[0], targetId) < 0:
                posting = self._iters[i].skipPosting(targetId)
                self._smallestPostings[i] = posting
                if posting[0] == targetId:
                    break    
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, posting[0]))
        return posting

class NOT(object):
    """
    The NOT operator is a negation query.  In our boolean logic implementation,
    this class is just a placeholder used by the query language parser; the real
    application logic is done in the Sieve class.
    """

    implements(IMatcher, IPostingList)

    def __init__(self, child):
        """
        :param child: A query whose results should be removed from the parent query.
        :type: An object implementing :class:`terane.bier.searching.IQuery`
        """
        self.child = child

    def __str__(self):
        return "<NOT %s>" % str(self.child)

    def optimizeMatcher(self, index):
        """
        Optimize the query.  If the child query optimizes out, then toss this query.

        :param index: The index we will be running the query on.
        :type index: Object implementing :class:`terane.bier.index.IIndex`
        :returns: The optimized query.
        :rtype: An object implementing :class:`terane.bier.searching.IQuery`
        """
        self.child = self.child.optimizeMatcher(index)
        if self.child == None:
            return None
        return self

    def matchesLength(self, searcher, startId, endId):
        """
        The NOT operator has no use for the matchesLength() method, and raises a
        NotImplemented exception if called.
        """
        raise NotImplemented()

    def iterMatches(self, searcher, startId, endId):
        """
        Returns an object for iterating through events matching the query.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.searching.IPostingList`
        """
        self._iter = self.child.iterMatches(searcher, startId, endId)
        return self

    def nextPosting(self):
        """
        Returns the docId of the next event matching the query, or None if there are no
        more events which match the query.

        :returns: The docId of the next matching event, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        posting = self._iter.nextPosting()
        logger.trace("%s: nextPosting() => %s" % (self, posting[0])) 
        return posting

    def skipPosting(self, targetId):
        """
        Returns the docId matching targetId if the query contains the specified targetId,
        or None if the targetId is not present.

        :returns: The docId matching the targetId, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        posting = self._iter.skipPosting(targetId)
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, posting[0]))
        return posting

class Sieve(object):
    """
    The Sieve class iterates through events returned by the source query, filtering
    out events if they are present in any of the filter queries.
    """

    implements(IMatcher, IPostingList)

    def __init__(self, source, filters):
        """
        :param source: The query which returns possible candidate events.
        :type source: An object implementing :class:`terane.bier.searching.IQuery`
        :param filters: The list of queries which return events to filter from the source results.
        :type filters: An object implementing :class:`terane.bier.searching.IQuery`
        """
        self.source = source
        self.filters = OR(filters)

    def __str__(self):
        return "<Sieve source=%s, filters=%s>" % (str(self.source), str(self.filters))

    def optimizeMatcher(self, index):
        """
        Optimize the query.

        :param index: The index we will be running the query on.
        :type index: Object implementing :class:`terane.bier.index.IIndex`
        :returns: The optimized query.
        :rtype: An object implementing :class:`terane.bier.searching.IQuery`
        """
        return self

    def matchesLength(self, searcher, startId, endId):
        """
        Returns an estimate of the approximate number of postings which will be returned
        for the query using the specified searcher within the specified period.  The estimate
        for a Sieve is the estimate of the source query.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: The postings length estimate.
        :rtype: int
        """
        return self.source.matchesLength(searcher, startId, endId)

    def iterMatches(self, searcher, startId, endId):
        """
        Returns an object for iterating through events matching the query.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.searching.IPostingList`
        """
        self._sourceIter = self.source.iterMatches(searcher, startId, endId)
        self._filterIter = self.filters.iterMatches(searcher, startId, endId)
        return self

    def nextPosting(self):
        """
        Returns the docId of the next event matching the query, or None if there are no
        more events which match the query.

        :returns: The docId of the next matching event, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        posting = (None,None,None)
        # loop until we find a docId not in the filter, or there are no more docIds
        while True:
            posting = self._sourceIter.nextPosting()
            # if there are not more docIds to retrieve from the source, we are done
            if posting[0] == None:
                break
            # if the the docId isn't present in the filter, then return it
            if posting[0] != self._filterIter.skipPosting(posting[0])[0]:
                break
        logger.trace("%s: nextPosting() => %s" % (self, posting[0])) 
        return posting

    def skipPosting(self, targetId):
        """
        Returns the docId matching targetId if the query contains the specified targetId,
        or None if the targetId is not present.

        :returns: The docId matching the targetId, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        # skip the source to the targetId
        posting = self._sourceIter.skipPosting(targetId)
        # if targetId is not in the source, then the skip fails
        if posting[0] != None:
            # if the targetId is present in the filter, then the skip fails
            if posting[0] == self._filterIter.skipPosting(posting[0])[0]:
                posting = (None,None,None)
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, posting[0]))
        return posting
