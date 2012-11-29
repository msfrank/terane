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

import bisect
from zope.interface import implements
from terane.bier.interfaces import IMatcher, IPostingList
from terane.bier.event import Contract
from terane.bier.evid import EVID
from terane.loggers import getLogger

logger = getLogger('terane.bier.matching')

class QueryTerm(object):

    implements(IMatcher)

    def __init__(self, fieldname, fieldtype, fieldfunc, value):
        """
        :param fieldname: The name of the field to search.
        :type fieldname: str
        :param value: The term to search for in the field.
        :type value: unicode
        """
        self.fieldname = fieldname
        self.fieldtype = fieldtype
        self.fieldfunc = fieldfunc
        self.value = value

    def __str__(self):
        if self.fieldname and self.fieldtype and self.fieldfunc:
            return "<QueryTerm %s=%s:%s(%s)>" % (self.fieldname,self.fieldtype,self.fieldfunc,self.value)
        if self.fieldname and self.fieldfunc:
            return "<QueryTerm %s=%s(%s)>" % (self.fieldname,self.fieldfunc,self.value)
        if self.fieldname :
            return "<QueryTerm %s='%s'>" % (self.fieldname,self.value)
        return "<QueryTerm '%s'>" % self.value

    def optimizeMatcher(self, index):
        schema = index.getSchema()
        try:
            field = schema.getField(self.fieldname, self.fieldtype)
        except KeyError:
            return None
        matcher = field.makeMatcher(self.fieldfunc, self.value)
        if not matcher:
            return None
        return matcher.optimizeMatcher(index)

    def matchesLength(searcher, startId, endId):
        raise NotImplementedError()

    def iterMatches(searcher, startId, endId):
        raise NotImplementedError()

class Term(object):
    """
    The basic query.  In order for an event to match, the term must be present in the
    specified field.
    """

    implements(IMatcher, IPostingList)

    def __init__(self, field, value):
        """
        :param field: The field to search.
        :type field: :class:`terane.bier.fields.QualifiedField`
        :param value: The string representation of the term to search for in the field.
        :type value: unicode
        """
        self.field = field
        self.value = value

    def __str__(self):
        return "<Term %s=%s>" % (self.field,self.value)

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
        return self

    def matchesLength(self, searcher, startId, endId):
        """
        Returns an estimate of the approximate number of matching postings which will
        be returned using the specified searcher within the specified period.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.ISearcher`
        :returns: The postings length estimate.
        :rtype: int
        """
        length = searcher.postingsLength(self.field, self.value, startId, endId)
        logger.trace("%s: postingsLength() => %i" % (self, length))
        return length

    def iterMatches(self, searcher, startId, endId):
        """
        Returns an object for iterating through matching postings.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.ISearcher`
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.IPostingList`
        """
        self._postings = searcher.iterPostings(self.field, self.value, startId, endId)
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

        :param targetId: The target event identifier.
        :type targetId: :class:`terane.bier.evid.EVID`
        :returns: The posting matching the targetId, or (None,None,None).
        :rtype: tuple
        """
        posting = self._postings.skipPosting(targetId)
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, posting[0]))
        return posting

    def close(self):
        self._postings.close()
        self._postings = None

class RangeGreaterThan(Term):
    """
    In order for an event to match, it must be greater than the term in the
    specified field.
    """

    implements(IMatcher, IPostingList)

    def __str__(self):
        return "<RangeGreaterThan %s=%s>" % (self.field,self.value)

    def matchesLength(self, searcher, startId, endId):
        length = searcher.postingsLengthBetween(self.field, self.value, None, startId, endId)
        logger.trace("%s: postingsLength() => %i" % (self, length))
        return length

    def iterMatches(self, searcher, startId, endId):
        self._postings = searcher.iterPostingsBetween(self.field, self.value, None, startId, endId)
        return self

class RangeLessThan(Term):
    """
    In order for an event to match, it must be less than the term in the
    specified field.
    """

    implements(IMatcher, IPostingList)

    def __str__(self):
        return "<RangeLessThan %s=%s>" % (self.field,self.value)

    def matchesLength(self, searcher, startId, endId):
        length = searcher.postingsLengthBetween(self.field, None, self.value, startId, endId)
        logger.trace("%s: postingsLength() => %i" % (self, length))
        return length

    def iterMatches(self, searcher, startId, endId):
        self._postings = searcher.iterPostingsBetween(self.field, None, self.value, startId, endId)
        return self

class Every(Term):
    """
    Matches every event within the specified period.
    """

    def __init__(self):
        pass
    def __str__(self):
        return "<Every>"

    def optimizeMatcher(self, index):
        """
        The Every matcher cannot be optimized, so it just returns itself.
        """
        return self

    def matchesLength(self, searcher, startId, endId):
        """
        Returns an estimate of the approximate number of matching postings which will
        be returned using the specified searcher within the specified period.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.ISearcher`
        :returns: The postings length estimate.
        :rtype: int
        """
        length = searcher.postingsLength(None, None, startId, endId)
        logger.trace("%s: postingsLength() => %i" % (self, length))
        return length


    def iterMatches(self, searcher, startId, endId):
        """
        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.ISearcher`
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.IPostingList`
        """
        self._postings = searcher.iterPostings(None, None, startId, endId)
        return self

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
        Returns the event identifier of the next event matching the query, or None if there are no
        more events which match the query.

        :returns: The event identifier of the next matching event, or None.
        :rtype: :class:`terane.bier.evid.EVID`
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
        Returns the event identifier matching targetId if the query contains the specified targetId,
        or None if the targetId is not present.

        :returns: The event identifier matching the targetId, or None.
        :rtype: :class:`terane.bier.evid.EVID`
        """
        posting = self._smallest.skipPosting(targetId)
        if posting[0] != None:
            for child in self._others:
                posting = child.skipPosting(targetId)
                if posting[0] == None:
                    break
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, posting[0]))
        return posting[0]

    def close(self):
        self._smallest.close()
        for i in self._others: i.close()
        self._smallest = None
        self._others = None

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
        self._iters = [child.iterMatches(searcher, startId, endId) for child in self.children]
        # lastId is the last evid returned by the iterator
        self._lastId = None
        # set our cmp function, which determines the next evid to return.  if we are searching in
        # reverse order, then reverse the meaning if the regular comparison function.
        if endId < startId:
            self._cmp = lambda d1,d2: cmp(d2,d1)
        # otherwise use the normal comparison function
        else:
            self._cmp = cmp
        return self

    def nextPosting(self):
        """
        Returns the event identifier of the next event matching the query, or None if there are no
        more events which match the query.

        :returns: The event identifier of the next matching event, or None.
        :rtype: :class:`terane.bier.evid.EVID`
        """
        curr = 0
        # check each child iter for the lowest evid
        for i in range(len(self.children)):
            # if None, then get the next posting from the iter
            if self._smallestPostings[i][0] == None:
                self._smallestPostings[i] = self._iters[i].nextPosting()
            # if the posting evid is None, then check the next iter
            if self._smallestPostings[i][0] == None:
                continue
            # if the evid equals the last evid returned, then ignore it
            if self._lastId != None and self._smallestPostings[i][0] == self._lastId:
                self._smallestPostings[i] = (None,None,None)
                continue
            # we don't compare the first evid with itself
            if i == 0:
                continue
            # if the evid is the current smallest evid, then remember it
            if self._smallestPostings[curr][0] == None or \
              self._cmp(self._smallestPostings[i][0], self._smallestPostings[curr][0]) < 0:
                curr = i
        # update lastId with the evid
        posting = self._smallestPostings[curr]
        self._lastId = posting[0]
        # forget the evid so we don't return it again
        self._smallestPostings[curr] = (None,None,None)
        logger.trace("%s: nextPosting() => %s" % (self, posting[0])) 
        return posting

    def skipPosting(self, targetId):
        """
        Returns the event identifier matching targetId if the query contains the specified targetId,
        or None if the targetId is not present.

        :returns: The event identifier matching the targetId, or None.
        :rtype: :class:`terane.bier.evid.EVID`
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

    def close(self):
        for i in self._iters: i.close()
        self._iters = None

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
        NotImplementedError exception if called.
        """
        raise NotImplementedError()

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
        Returns the event identifier of the next event matching the query, or None if there are no
        more events which match the query.

        :returns: The event identifier of the next matching event, or None.
        :rtype: :class:`terane.bier.evid.EVID`
        """
        posting = self._iter.nextPosting()
        logger.trace("%s: nextPosting() => %s" % (self, posting[0])) 
        return posting

    def skipPosting(self, targetId):
        """
        Returns the event identifier matching targetId if the query contains
        the specified targetId, or None if the targetId is not present.

        :returns: The event identifier matching the targetId, or None.
        :rtype: :class:`terane.bier.evid.EVID`
        """
        posting = self._iter.skipPosting(targetId)
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, posting[0]))
        return posting

    def close(self):
        self._iter.close()
        self._iter = None

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
        Returns the event identifier of the next event matching the query, or None
        if there are no more events which match the query.

        :returns: The event identifier of the next matching event, or None.
        :rtype: :class:`terane.bier.evid.EVID`
        """
        posting = (None,None,None)
        # loop until we find a evid not in the filter, or there are no more evids
        while True:
            posting = self._sourceIter.nextPosting()
            # if there are not more evids to retrieve from the source, we are done
            if posting[0] == None:
                break
            # if the the evid isn't present in the filter, then return it
            if posting[0] != self._filterIter.skipPosting(posting[0])[0]:
                break
        logger.trace("%s: nextPosting() => %s" % (self, posting[0])) 
        return posting

    def skipPosting(self, targetId):
        """
        Returns the event identifier matching targetId if the query contains
        the specified targetId, or None if the targetId is not present.

        :returns: The event identifier matching the targetId, or None.
        :rtype: :class:`terane.bier.evid.EVID`
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

    def close(self):
        self._sourceIter.close()
        self._filterIter.close()
        self._filterIter = None
        self._sourceIter = None

class Phrase(object):
    """
    The phrase query matcher.  In order for an event to match, it must match all
    child term matchers, and each term must appear in the appropriate position.
    """

    implements(IMatcher, IPostingList)

    def __init__(self, field, terms):
        """
        :param phrase: A list of child term matchers.
        :type phrase: list of strings
        """
        self.field = field
        self.terms = terms
        self._lengths = None
        self._iters = None

    def __str__(self):
        return "<Phrase %s>" % self.terms

    def optimizeMatcher(self, index):
        """
        Optimize the matcher.  If the field doesn't exist in the schema, then toss the matcher.

        :param index: The index we will be running the query on.
        :type index: Object implementing :class:`terane.bier.IIndex`
        :returns: The optimized matcher.
        :rtype: An object implementing :class:`terane.bier.IMatcher`
        """
        if len(self.terms) == 0:
            return None
        if len(self.terms) == 1:
            return Term(self.field, self.terms[0]).optimizeMatcher(index)
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
        for position in range(len(self.terms)):
            term = self.terms[position]
            length = searcher.postingsLength(self.field, term, startId, endId)
            bisect.insort_right(self._lengths, (length,term,position))
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
        self._iters = [searcher.iterPostings(self.field, v[1], startId, endId) for v in self._lengths]
        return self

    def nextPosting(self):
        """
        Returns the event identifier of the next event matching the query, or None if there are no
        more events which match the query.

        :returns: The event identifier of the next matching event, or None.
        :rtype: :class:`terane.bier.evid.EVID`
        """
        def _get():
            while True:
                postings = []
                smallest = 0
                posting = self._iters[0].nextPosting()
                if posting == (None, None, None):
                    return posting
                postings.append(posting)
                for i in range(len(self._iters))[1:]:
                    posting = self._iters[i].skipPosting(postings[0][0])
                    if posting == (None, None, None):
                        break
                    postings.append(posting)
                if len(postings) != len(self._iters):
                    continue
                if self._positionsMatch(postings) == True:
                    return posting
        posting = _get()
        logger.trace("%s: nextPosting() => %s" % (self, posting[0])) 
        return posting

    def skipPosting(self, targetId):
        """
        Returns the event identifier matching targetId if the query contains the specified targetId,
        or None if the targetId is not present.

        :returns: The event identifier matching the targetId, or None.
        :rtype: :class:`terane.bier.evid.EVID`
        """
        def _skip():
            postings = []
            smallest = 0
            for i in range(len(self._iters)):
                posting = self._iters[i].skipPosting(targetId)
                if posting[0] == None:
                    return (None, None, None)
                postings.append(posting)
            if self._positionsMatch(postings) == True:
                return posting[0]
            return (None, None, None)
        posting = _skip()
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, posting[0]))
        return posting

    def _positionsMatch(self, postings):
        """
        Returns True if the posting positions line up, otherwise False.
        """
        positions = []
        for i in range(len(postings)):
            positions.append((self._lengths[i][2], postings[i][1]['pos']))
        positions = map(lambda x: x[1], sorted(positions))
        logger.trace("%s: _positionsMatch(%s): positions=%s" % (self, postings[0][0], positions))
        for firstPos in positions[0]:
            positionsMatch = True
            for i in range(1, len(positions)):
                if (firstPos + i) not in positions[i]:
                    positionsMatch = False
                    break
            if positionsMatch == True:
                return True
        return False

    def close(self):
        self._lengths = None
        for i in self._iters: i.close()
        self._iters = None
