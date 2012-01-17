import time, datetime, copy, bisect
from zope.interface import Interface, implements
from terane.bier.docid import DocID
from terane.loggers import getLogger

logger = getLogger('terane.bier.searching')

class IPostingList(Interface):
    def nextPosting():
        "Returns the next docId, or raises StopIteration if finished."
    def skipPosting(targetId):
        "Skips to the targetId, or raises StopIteration if finished."

class IQuery(Interface):
    def optimizeQuery(index):
        "Returns an optimized query."
    def postingsLength(searcher, period):
        "Returns an estimate of the number of postings within the specified period."
    def iterPostings(searcher, period, reverse):
        """
        Returns an object implementing IPostingList which yields docIds for each
        posting matching the query within the specified period.
        """

class ISearcher(Interface):
    def __enter__():
        "Enter the transactional context."
    def postingsLength(fieldname, term, period):
        "Returns the maximum number of possible postings for the term in the specified field."
    def iterPostings(fieldname, term, period, reverse):
        """
        Returns an object implementing IPostingList which yields docIds for each
        posting of the term in the specified field.
        """
    def getEvent(docId):
        "Returns the event specified by docId."
    def __exit__(excType, excValue, traceback):
        "Exit the transactional context."

class SearcherError(Exception):
    pass

class Period(object):
    """
    A time range within which to constain a query.
    """
    def __init__(self, start, end, startexcl, endexcl, epoch=None):
        """
        :param start: The start of the time range.
        :type start: :class:`datetime.datetime`
        :param end: The end of the time range.
        :type end: :class:`datetime.datetime`
        :param startexcl: If True, then the start of the range is exclusive.
        :type startexcl: bool
        :param endexcl: If True, then the end of the range is exclusive.
        :type endexcl: bool
        :param epoch:
        :type epoch: 
        """
        self.start = int(time.mktime(start.timetuple()))
        self.end = int(time.mktime(end.timetuple()))
        self.startexcl = startexcl
        self.endexcl = endexcl
        self._epoch = epoch

    def __str__(self):
        start = time.strftime("%a, %d %b %Y %H:%M:%S UTC", time.gmtime(self.start))
        end = time.strftime("%a, %d %b %Y %H:%M:%S UTC", time.gmtime(self.end))
        return "<Period from %s to %s>" % (start, end)

    def getEpoch(self):
        return self._epoch

    def setEpoch(self, epoch):
        self._epoch = epoch

    def startingID(self):
        if self.startexcl:
            return DocID(self.start + 1, 0, 0)
        return DocID(self.start, 0, 0)

    def endingID(self):
        if self.endexcl:
            return DocID(self.end - 1, 2**32 - 1, 2**64 - 1)
        return DocID(self.end, 2**32 - 1, 2**64 - 1)

class Term(object):
    """
    The basic query.  In order for an event to match, the term must be present in the
    specified field.
    """

    implements(IQuery, IPostingList)

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

    def optimizeQuery(self, index):
        """
        Optimize the query.  If the field does not exist in the index, then toss the
        query.  If the normalized term value splits into multiple terms, then return
        the union of the terms.

        :param index: The index we will be running the query on.
        :type index: Object implementing :class:`terane.bier.index.IIndex`
        :returns: The optimized query.
        :rtype: An object implementing :class:`terane.bier.searching.IQuery`
        """
        schema = index.schema()
        if not schema.has(self.fieldname):
            return None
        field = schema.get(self.fieldname)
        terms = list([term for term,_ in field.terms(self.value)])
        if len(terms) == 0:
            return None
        elif len(terms) > 1:
            return OR([Term(self.fieldname,t).optimizeQuery(index) for t in terms])
        self.value = terms[0]
        return self

    def postingsLength(self, searcher, period):
        """
        Returns an estimate of the approximate number of postings which will be returned
        for the query using the specified searcher within the specified period.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: The postings length estimate.
        :rtype: int
        """
        length = searcher.postingsLength(self.fieldname, self.value, period)
        logger.trace("%s: postingsLength() => %i" % (self, length))
        return length

    def iterPostings(self, searcher, period, reverse):
        """
        Returns an object for iterating through events matching the query.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.searching.IPostingList`
        """
        self._postings = searcher.iterPostings(self.fieldname, self.value, period, reverse)
        return self

    def nextPosting(self):
        """
        Returns the docId of the next event matching the query, or None if there are no
        more events which match the query.

        :returns: The docId of the next matching event, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        docId = self._postings.nextPosting()[0]
        logger.trace("%s: nextPosting() => %s" % (self, docId)) 
        return docId

    def skipPosting(self, targetId):
        """
        Returns the docId matching targetId if the query contains the specified targetId,
        or None if the targetId is not present.

        :returns: The docId matching the targetId, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        docId = self._postings.skipPosting(targetId)[0]
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, docId))
        return docId

class AND(object):
    """
    The AND operator is an intersection query.  In order for an event to match, it must
    match all child queries.
    """

    implements(IQuery, IPostingList)

    def __init__(self, children):
        """
        :param children: A list of child queries which are to be intersected.
        :type children: list
        """
        self.children = children
        self._lengths = None

    def __str__(self):
        return "<AND [%s]>" % ', '.join([str(child) for child in self.children])

    def optimizeQuery(self, index):
        """
        Optimize the query.  If any child queries are AND operators, then move their
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
            # optimize each child query
            child = child.optimizeQuery(index)
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
        # if there are no children, then we can toss this query too
        if len(self.children) == 0:
            return None
        # if there are any NOT operators, then we wrap this query and the combined NOTs in a Sieve.
        if len(excludes) > 0:
            return Sieve(self, excludes)
        return self

    def postingsLength(self, searcher, period):
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
            bisect.insort_right(self._lengths, (child.postingsLength(searcher, period),child))
        length = self._lengths[0][0]
        logger.trace("%s: postingsLength() => %i" % (self, length))
        return length

    def iterPostings(self, searcher, period, reverse):
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
            self.postingsLength(searcher, period)
        self._smallest = self._lengths[0][1].iterPostings(searcher, period, reverse)
        self._others = [child[1].iterPostings(searcher, period, reverse) for child in self._lengths[1:]]
        return self

    def nextPosting(self):
        """
        Returns the docId of the next event matching the query, or None if there are no
        more events which match the query.

        :returns: The docId of the next matching event, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        while True:
            docId = self._smallest.nextPosting()
            if docId == None:
                logger.trace("%s: nextPosting() => %s" % (self, docId)) 
                return docId
            for child in self._others:
                if child.skipPosting(docId) == None:
                    docId = None
                    break
            if docId != None:
                logger.trace("%s: nextPosting() => %s" % (self, docId)) 
                return docId

    def skipPosting(self, targetId):
        """
        Returns the docId matching targetId if the query contains the specified targetId,
        or None if the targetId is not present.

        :returns: The docId matching the targetId, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        docId = self._smallest.skipPosting(targetId)
        if docId != None:
            for child in self._others:
                docId = child.skipPosting(targetId)
                if docId == None:
                    break
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, docId))
        return docId

class OR(object):
    """
    The OR operator is a union query.  In order for an event to match, it must
    match at least one of the child queries.
    """

    implements(IQuery, IPostingList)

    def __init__(self, children):
        """
        :param children: A list of child queries which are to be unioned.
        :type children: list
        """
        self.children = children

    def __str__(self):
        return "<OR [%s]>" % ', '.join([str(child) for child in self.children])

    def optimizeQuery(self, index):
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
            # optimize each child query
            child = child.optimizeQuery(index)
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
        # if there are no children, then we can toss this query too
        if len(self.children) == 0:
            return None
        # if there are any NOT operators, then we wrap this query and the combined NOTs in a Sieve.
        if len(excludes) > 0:
            return Sieve(self, excludes)
        return self

    def postingsLength(self, searcher, period):
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
            length += child.postingsLength(searcher, period)
        return length

    def iterPostings(self, searcher, period, reverse):
        """
        Returns an object for iterating through events matching the query.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.searching.IPostingList`
        """
        # smallestIds contains the smallest docId for each child query, or None 
        self._smallestIds = [None for i in range(len(self.children))]
        # iters contains the iterator (object implementing IPostingList) for each child query
        self._iters = [child.iterPostings(searcher,period,reverse) for child in self.children]
        # lastId is the last docId returned by the iterator
        self._lastId = None
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
            # if None, then get the next docId from the iter
            if self._smallestIds[i] == None:
                self._smallestIds[i] = self._iters[i].nextPosting()
            # if the docId is None, then check the next iter
            if self._smallestIds[i] == None:
                continue
            # if the docId equals the last docId returned, then ignore it
            if self._lastId != None and self._smallestIds[i] == self._lastId:
                self._smallestIds[i] = None
                continue
            # we don't compare the first docId with itself
            if i == 0:
                continue
            # if the docId is the current smallest docId, then remember it
            if self._smallestIds[curr] == None or self._smallestIds[i] < self._smallestIds[curr]:
                curr = i
        # update lastId with the docId
        docId = self._lastId = self._smallestIds[curr]
        # forget the docId so we don't return it again
        self._smallestIds[curr] = None
        logger.trace("%s: nextPosting() => %s" % (self, docId)) 
        return docId

    def skipPosting(self, targetId):
        """
        Returns the docId matching targetId if the query contains the specified targetId,
        or None if the targetId is not present.

        :returns: The docId matching the targetId, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        docId = None
        # iterate through each child query
        for i in range(len(self.children)):
            docId = self._smallestIds[i]
            # if the smallestId equals the targetId, we are done
            if docId == targetId:
                break
            # otherwise check if the targetId exists in the child query
            if docId == None or docId < targetId:
                docId = self._iters[i].skipPosting(targetId)
                self._smallestIds[i] = docId
                if docId == targetId:
                    break    
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, docId))
        return docId

class NOT(object):
    """
    The NOT operator is a negation query.  In our boolean logic implementation,
    this class is just a placeholder used by the query language parser; the real
    application logic is done in the Sieve class.
    """

    implements(IQuery, IPostingList)

    def __init__(self, child):
        """
        :param child: A query whose results should be removed from the parent query.
        :type: An object implementing :class:`terane.bier.searching.IQuery`
        """
        self.child = child

    def __str__(self):
        return "<NOT %s>" % str(self.child)

    def optimizeQuery(self, index):
        """
        Optimize the query.  If the child query optimizes out, then toss this query.

        :param index: The index we will be running the query on.
        :type index: Object implementing :class:`terane.bier.index.IIndex`
        :returns: The optimized query.
        :rtype: An object implementing :class:`terane.bier.searching.IQuery`
        """
        self.child = self.child.optimizeQuery(index)
        if self.child == None:
            return None
        return self

    def postingsLength(self, searcher, period):
        """
        The NOT operator has no use for the postingsLength() method, and raises a
        NotImplemented exception if called.
        """
        raise NotImplemented()

    def iterPostings(self, searcher, period, reverse):
        """
        Returns an object for iterating through events matching the query.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.searching.IPostingList`
        """
        self._iter = self.child.iterPostings(searcher, period, reverse)
        return self

    def nextPosting(self):
        """
        Returns the docId of the next event matching the query, or None if there are no
        more events which match the query.

        :returns: The docId of the next matching event, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        docId = self._iter.nextPosting()
        logger.trace("%s: nextPosting() => %s" % (self, docId)) 
        return docId

    def skipPosting(self, targetId):
        """
        Returns the docId matching targetId if the query contains the specified targetId,
        or None if the targetId is not present.

        :returns: The docId matching the targetId, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        docId = self._iter.skipPosting(targetId)
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, docId))
        return docId

class Sieve(object):
    """
    The Sieve class iterates through events returned by the source query, filtering
    out events if they are present in any of the filter queries.
    """

    implements(IQuery, IPostingList)

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

    def optimizeQuery(self, index):
        """
        Optimize the query.

        :param index: The index we will be running the query on.
        :type index: Object implementing :class:`terane.bier.index.IIndex`
        :returns: The optimized query.
        :rtype: An object implementing :class:`terane.bier.searching.IQuery`
        """
        return self

    def postingsLength(self, searcher, period):
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
        return self.source.postingsLength(searcher, period)

    def iterPostings(self, searcher, period, reverse):
        """
        Returns an object for iterating through events matching the query.

        :param searcher: A handle to the index we are searching.
        :type searcher: An object implementing :class:`terane.bier.searching.ISearcher`
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: An object for iterating through events matching the query.
        :rtype: An object implementing :class:`terane.bier.searching.IPostingList`
        """
        self._sourceIter = self.source.iterPostings(searcher, period, reverse)
        self._filterIter = self.filters.iterPostings(searcher, period, reverse)
        return self

    def nextPosting(self):
        """
        Returns the docId of the next event matching the query, or None if there are no
        more events which match the query.

        :returns: The docId of the next matching event, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        docId = None
        # loop until we find a docId not in the filter, or there are no more docIds
        while True:
            docId = self._sourceIter.nextPosting()
            # if there are not more docIds to retrieve from the source, we are done
            if docId == None:
                break
            # if the the docId isn't present in the filter, then return it
            if docId != self._filterIter.skipPosting(docId):
                break
        logger.trace("%s: nextPosting() => %s" % (self, docId)) 
        return docId

    def skipPosting(self, targetId):
        """
        Returns the docId matching targetId if the query contains the specified targetId,
        or None if the targetId is not present.

        :returns: The docId matching the targetId, or None.
        :rtype: :class:`terane.bier.docid.DocID`
        """
        # skip the source to the targetId
        docId = self._sourceIter.skipPosting(targetId)
        # if targetId is not in the source, then the skip fails
        if docId != None:
            # if the targetId is present in the filter, then the skip fails
            if docId == self._filterIter.skipPosting(docId):
                docId = None
        logger.trace("%s: skipPosting(%s) => %s" % (self, targetId, docId))
        return docId

class Results(object):
    """
    Contains the results of a query.  Results is iterable, and supports indexing.
    Each item in the results list is a tuple of the form (docId, fields), where
    docId is a string representation of the event docId, and fields is a dict mapping
    fieldnames to field values.  the Results object also has a attribute 'fields'
    which is a list of names of each field present in the results list.
    """

    def __init__(self, results, fields):
        self.meta = dict()
        self._results = results
        self.fields = fields

    def __len__(self):
        return len(self._results)

    def __iter__(self):
        return iter(self._results)

    def __getitem__(self, i):
        return self._results[i]

def searchIndices(indices, query, period, reverse=False, fields=None, limit=100):
    """
    Search the specified indices for events matching the specified query.

    :param indices: A list of indices to search.
    :type indices: A list of objects implementing :class:`terane.bier.index.IIndex`
    :param query: The programmatic query to use for searching the indices.
    :type query: An object implementing :class:`terane.bier.searching.IQuery`
    :param period: The period within which the search is constrained.
    :type period: :class:`terane.bier.searching.Period`
    :param reverse: If True, then reverse the order of events.
    :type reverse: bool
    :param fields: If not None, then only return the specified fields of each event. 
    :type fields: list or None
    :param limit: Only returned the specified number of events.
    :type limit: int
    """
    postings = []
    # search each index separately, then merge the results
    for index in indices:
        # we create a copy of the original query, which can possibly be optimized
        # with index-specific knowledge.
        _query = copy.deepcopy(query).optimizeQuery(index)
        logger.debug("optimized query for index '%s': %s" % (index.name,str(_query)))
        # if the query optimized out entirely, then skip to the next index
        if _query == None:
            continue
        # iterate through the search results
        searcher = index.searcher()
        piter = _query.iterPostings(searcher, period, reverse)
        i = 0
        # we terminate the search prematurely if we have reached the results limit
        while i < limit:
            docId = piter.nextPosting()
            if docId == None:
                break
            logger.trace("found event %s" % docId)
            # remember the docId and the searcher it came from, so we can retrieve
            # the full event after the final sort.
            postings.append((docId, searcher))
            i += 1
    # perform a sort on the docIds, which orders them naturally by date
    postings.sort()
    foundfields = []
    results = []
    # retrieve the full event for each docId
    for docId,searcher in postings[:limit]:
        event = searcher.getEvent(docId)
        # keep a record of all field names found in the results.
        for fieldname in event.keys():
            if fieldname not in foundfields: foundfields.append(fieldname)
        # filter out unwanted fields
        if fields != None:
            event = dict([(k,v) for k,v in event.items() if k in fields])
        results.append((str(docId), event))
    return Results(results, foundfields)
