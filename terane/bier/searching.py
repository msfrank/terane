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
from terane.bier.docid import DocID
from terane.loggers import getLogger

logger = getLogger('terane.bier.searching')

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
        _query = copy.deepcopy(query).optimizeMatcher(index)
        logger.debug("optimized query for index '%s': %s" % (index.name,str(_query)))
        # if the query optimized out entirely, then skip to the next index
        if _query == None:
            continue
        # iterate through the search results
        searcher = index.searcher()
        postingList = _query.iterMatches(searcher, period, reverse)
        i = 0
        # we terminate the search prematurely if we have reached the results limit
        while i < limit:
            posting = postingList.nextPosting()
            if posting[0] == None:
                break
            logger.trace("found event %s" % posting[0])
            # remember the docId and the searcher it came from, so we can retrieve
            # the full event after the final sort.
            postings.append(posting)
            i += 1
    # perform a sort on the docIds, which orders them naturally by date
    postings.sort()
    foundfields = []
    results = []
    # retrieve the full event for each docId
    for docId,tvalue,store in postings[:limit]:
        event = store.getEvent(docId)
        # keep a record of all field names found in the results.
        for fieldname in event.keys():
            if fieldname not in foundfields: foundfields.append(fieldname)
        # filter out unwanted fields
        if fields != None:
            event = dict([(k,v) for k,v in event.items() if k in fields])
        results.append((str(docId), event))
    return Results(results, foundfields)
