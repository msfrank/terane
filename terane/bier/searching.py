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
            self.start = EVID(int(calendar.timegm(start.timetuple())), 0, 0)
        elif isinstance(start, EVID):
            self.start = start
        else:
            raise TypeError("start must be datetime.datetime or terane.bier.evid.EVID")
        if isinstance(end, datetime.datetime):
            self.end = EVID(int(calendar.timegm(end.timetuple())), 0, 0)
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

class ResultSet(object):
    def __init__(self, searchers, postingLists, start, reverse, fields, limit):
        self._searchers = searchers
        self._postingLists = postingLists
        self._currPostings = [(None,None,None) for i in range(len(postingLists))]
        self._start = start
        self._lastId = None
        if reverse:
            self._cmp = lambda x,y: cmp(y,x) 
        else:
            self._cmp = cmp
        self._fields = fields
        self._limit = limit
        self._count = 0
        self.events = []
        self.fields = []
        self.runtime = 0.0

    def __iter__(self):
        return self

    def next(self):
        try:
            nlists = len(self._postingLists)
            if nlists == 0:
                raise StopIteration()
            # check each child iter for the lowest evid
            curr = 0
            for i in range(nlists):
                # if the postingList is None, then its closed
                if self._postingLists[i] == None:
                    continue
                # if current posting for this posting list was consumed, then
                # get the next posting from the posting list
                if self._currPostings[i] == (None,None,None):
                    self._currPostings[i] = self._postingLists[i].nextPosting()
                # if the next posting for this posting list is (None,None,None),
                # then we are done with this posting list
                if self._currPostings[i] == (None,None,None):
                    self._postingLists[i] = None
                    continue
                # if the evid equals the last evid returned, then ignore it
                if self._lastId != None and self._currPostings[i][0] == self._lastId:
                    self._currPostings[i] = (None,None,None)
                    continue
                # we don't compare the first evid with itself
                if i == 0:
                    continue
                # if the evid is the current smallest evid, then remember it
                if self._currPostings[curr] == (None,None,None) or \
                  self._cmp(self._currPostings[i][0], self._currPostings[curr][0]) < 0:
                    curr = i
            # stop iterating if there are no more results
            if self._currPostings[curr] == (None,None,None):
                raise StopIteration()
            evid,tvalue,store = self._currPostings[curr]
            # remember the last evid
            self._lastId = evid
            # forget the evid so we don't return it again
            self._currPostings[curr] = (None,None,None)
            # retrieve the event
            if not IEventStore.providedBy(store):
                raise TypeError("store does not implement IEventStore")
            defaultfield, defaultvalue, fields = store.getEvent(evid)
            if defaultfield not in self.fields:
                self.fields.append(defaultfield)
            # keep a record of all field names found in the results.
            for fieldname in fields.keys():
                if fieldname not in self.fields:
                    self.fields.append(fieldname)
            # filter out unwanted fields
            if self._fields != None:
                fields = dict([(k,v) for k,v in fields.items() if k in self._fields])
            self.events.append((str(evid), defaultfield, defaultvalue, fields))
            logger.trace("added event %s to resultset" % evid)
            # if we have reached our limit
            self._count += 1
            if self._count == self._limit:
                raise StopIteration()
        except:
            self.close()
            self.runtime = time.time() - self._start
            logger.trace("retrieved %i events in %f seconds" % (len(self.events),self.runtime))
            raise

    def close(self):
        for postingList in self._postingLists:
            if postingList != None: postingList.close()
        self._postingLists = None
        for searcher in self._searchers:
            searcher.close()
        self._searchers = None

def searchIndices(indices, query, period, lastId=None, reverse=False, fields=None, limit=100):
    """
    Search the specified indices for events matching the specified query.

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
    :returns: A CooperativeTask which contains a Deferred and manages the search task.
    :rtype: :class:`twisted.internet.task.CooperativeTask`
    """
    start = time.time()
    # determine the evids to use as start and end keys
    if reverse == False:
        startId, endId = period.getRange()
    else:
        endId, startId = period.getRange()
    if lastId != None:
        if not lastId in period:
            raise SearcherError("lastId %s is not within period" % lastId)
        startId = lastId
    # search each index separately, then merge the results
    try:
        searchers = []
        postingLists = []
        for index in indices:
            if not IIndex.providedBy(index):
                raise TypeError("index does not implement IIndex")
            # we create a copy of the original query, which can possibly be optimized
            # with index-specific knowledge.
            _query = copy.deepcopy(query)
            try:
                _query = _query.optimizeMatcher(index)
            except NotImplementedError, e:
                raise SearcherError(str(e))
            logger.debug("optimized query for index '%s': %s" % (index.name,str(_query)))
            # if the query optimized out entirely, then skip to the next index
            if _query == None:
                continue
            # get the posting list to iterate through
            searcher = index.newSearcher()
            if not ISearcher.providedBy(searcher):
                raise TypeError("searcher does not implement ISearcher")
            postingList = _query.iterMatches(searcher, startId, endId)
            if not IPostingList.providedBy(postingList):
                raise TypeError("posting list does not implement IPostingList")
            searchers.append(searcher)
            postingLists.append(postingList)
        # return a cooperative task
        return cooperate(ResultSet(searchers, postingLists, start, reverse, fields, limit))
    except BaseException, e:
        if not isinstance(e, SearcherError):
            logger.exception(e)
        # free all held resources since we aren't passing them to ResultSet
        for postingList in postingLists: postingList.close()
        for searcher in searchers: searcher.close()
        raise
