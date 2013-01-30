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

import time, datetime, calendar, copy
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import cooperate
from terane.bier.interfaces import IIndex, ISearcher, IPostingList, IEventStore
from terane.bier.evid import EVID
from terane.loggers import getLogger

logger = getLogger('terane.bier.searching')

class SearcherError(Exception):
    pass

class Period(object):
    """
    A time range within which to constain a query.
    """
    def __init__(self, start, end, startexcl, endexcl):
        """
        :param start: The start of the time range.
        :type start: :class:`datetime.datetime`
        :param end: The end of the time range.
        :type end: :class:`datetime.datetime`
        :param startexcl: If True, then the start of the range is exclusive.
        :type startexcl: bool
        :param endexcl: If True, then the end of the range is exclusive.
        :type endexcl: bool
        """
        if isinstance(start, datetime.datetime):
            self.start = EVID.fromDatetime(start)
        elif isinstance(start, EVID):
            self.start = start
        else:
            raise TypeError("start must be datetime.datetime or terane.bier.evid.EVID")
        if isinstance(end, datetime.datetime):
            self.end = EVID.fromDatetime(end)
        elif isinstance(end, EVID):
            self.end = end
        else:
            raise TypeError("end must be datetime.datetime or terane.bier.evid.EVID")
        self.startexcl = startexcl
        self.endexcl = endexcl

    def __contains__(self, evid):
        if not isinstance(evid, EVID):
            raise TypeError()
        if not self.startexcl:
            if evid < self.start: return False
        else:
            if evid <= self.start: return False
        if not self.endexcl:
            if evid < self.end: return False
        else:
            if evid >= self.end: return False
        return True

    def __str__(self):
        if self.start.ts >= 0 and self.start.ts < 2**31:
            start = time.strftime("%a, %d %b %Y %H:%M:%S UTC", time.gmtime(self.start.ts))
        else:
            start = "0x%x" % self.start.ts
        if self.end.ts >= 0 and self.end.ts < 2**31:
            end = time.strftime("%a, %d %b %Y %H:%M:%S UTC", time.gmtime(self.end.ts))
        else:
            end = "0x%x" % self.end.ts
        return "<Period from %s to %s>" % (start, end)

    def getRange(self):
        start = self.start
        if self.startexcl == True:
            start = start + 1
        end = self.end
        if self.endexcl == True:
            end = end - 1
        return start, end

class SearcherWorker(object):
    """
    A worker which searches the specified indices using the supplied query.
    Instances of this class must be submitted to a :class:`terane.sched.Task`
    to be scheduled. 
    """

    def __init__(self, indices, query, period, lastId=None, reverse=False, fields=None, limit=100):
        """
        :param indices: A list of indices to search.
        :type indices: A list of objects implementing :class:`terane.bier.index.IIndex`
        :param query: The programmatic query to use for searching the indices.
        :type query: An object implementing :class:`terane.bier.searching.IQuery`
        :param period: The period within which the search is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :param lastId: The real key to start iterating from.
        :type lastId: :class:`terane.bier.evid.EVID`
        :param reverse: If True, then reverse the order of events.
        :type reverse: bool
        :param fields: If not None, then only return the specified fields of each event. 
        :type fields: list or None
        :param limit: Only returned the specified number of events.
        :type limit: int
        """
        # determine the evids to use as start and end keys
        if reverse == False:
            self._startId, self._endId = period.getRange()
        else:
            self._endId, self._startId = period.getRange()
        if lastId != None:
            if not lastId in period:
                raise SearcherError("lastId %s is not within period" % lastId)
            self._startId = lastId
        self._lastId = lastId
        for index in indices:
            if not IIndex.providedBy(index):
                raise TypeError("one or more indices does not implement IIndex")
        self._indices = indices
        self._query = query
        self._reverse = reverse
        self._fields = fields
        self._limit = limit
        self.events = []
        self.fields = []
        self.runtime = 0.0

    def next(self):
        start = time.time()
        searchers = []
        postingLists = []
        try:
            # get a searcher and posting list for each index
            for index in self._indices:
                # we create a copy of the original query, which can possibly be optimized
                # with index-specific knowledge.
                query = copy.deepcopy(self._query)
                # get the posting list to iterate through
                searcher = yield index.newSearcher()
                if not ISearcher.providedBy(searcher):
                    raise TypeError("searcher does not implement ISearcher")
                query = yield query.optimizeMatcher(searcher)
                logger.debug("optimized query for index '%s': %s" % (index.name,str(query)))
                # if the query optimized out entirely, then skip to the next index
                if query == None:
                    yield searcher.close()
                    continue
                postingList = yield query.iterMatches(searcher, self._startId, self._endId)
                if not IPostingList.providedBy(postingList):
                    raise TypeError("posting list does not implement IPostingList")
                searchers.append(searcher)
                postingLists.append(postingList)
            # loop forever until we reach the search limit, we exhaust all of our
            # posting lists, or we encounter an exception
            currPostings = [(None,None,None) for i in range(len(postingLists))]
            smallestList = 0
            lastId = None
            compar = (lambda x,y: cmp(y,x)) if self._reverse else cmp 
            while True:
                # if we have reached our limit
                if len(self.events) == self._limit:
                    self.runtime = time.time() - start
                    raise StopIteration()
                # check each child iter for the lowest evid
                for currList in range(len(postingLists)):
                    if currList == None:
                        smallestList = 0
                    # if the postingList is None, then its closed
                    if postingLists[currList] == None:
                        # FIXME: close the posting list and searcher
                        continue
                    # if current posting for this posting list was not consumed
                    if currPostings[currList] != (None,None,None):
                        continue
                    # otherwise get the next posting from the posting list
                    currPostings[currList] = yield postingLists[currList].nextPosting()
                    # if the next posting for this posting list is (None,None,None),
                    # then we are done with this posting list
                    if currPostings[currList] == (None,None,None):
                        postingList = postingLists[currList]
                        yield postingList.close()
                        postingLists[currList] = None
                        continue
                    # if the evid equals the last evid returned, then ignore it
                    if lastId != None and currPostings[currList][0] == lastId:
                        currPostings[currList] = (None,None,None)
                        continue
                    # we don't compare the first evid with itself
                    if currList == 0:
                        continue
                    # if the evid is the current smallest evid, then remember it
                    if (currPostings[smallestList] == (None,None,None) or
                      compar(currPostings[currList][0], currPostings[smallestList][0]) < 0):
                        smallestList = currList
                # get the next posting
                currList = None
                evid,_,store = currPostings[smallestList]
                # stop iterating if there are no more results
                if evid == None:
                    self.runtime = time.time() - start
                    raise StopIteration()
                # remember the last evid
                lastId = evid
                # forget the evid so we don't return it again
                currPostings[smallestList] = (None,None,None)
                # retrieve the event
                if not IEventStore.providedBy(store):
                    raise TypeError("store does not implement IEventStore")
                event = yield store.getEvent(evid)
                defaultfield, defaultvalue, fields = event
                if defaultfield not in self.fields:
                    self.fields.append(defaultfield)
                # keep a record of all field names found in the results
                for fieldname in fields.keys():
                    if fieldname not in self.fields:
                        self.fields.append(fieldname)
                # filter out unwanted fields
                if self._fields != None:
                    fields = dict([(k,v) for k,v in fields.items() if k in self._fields])
                self.events.append(((evid.ts,evid.offset), defaultfield, defaultvalue, fields))
                logger.trace("added event %s to results" % evid)
        finally:
            for postingList in postingLists:
                if postingList != None:
                    yield postingList.close()
            for searcher in searchers:
                yield searcher.close()
