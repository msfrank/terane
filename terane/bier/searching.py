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
from terane.bier import ISearcher, IPostingList, IEventStore
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
    :returns: The list of results, and the list of field names present in the results.
    :rtype: tuple
    """
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
    postings = []
    searchers = []
    try:
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
            if not ISearcher.providedBy(searcher):
                raise TypeError("searcher does not implement ISearcher")
            searchers.append(searcher)
            postingList = _query.iterMatches(searcher, startId, endId)
            if not IPostingList.providedBy(postingList):
                raise TypeError("posting list does not implement IPostingList")
            i = 0
            # we terminate the search prematurely if we have reached the results limit
            while i < limit:
                posting = postingList.nextPosting()
                if posting[0] == None:
                    break
                logger.trace("found event %s" % posting[0])
                # remember the evid and the store it came from, so we can retrieve
                # the full event after the final sort.
                postings.append(posting)
                i += 1
            postingList.close()
        # perform a sort on the evids, which orders them naturally by date
        postings.sort(reverse=reverse)
        results = []
        foundfields = []
        # retrieve the full event for each evid
        for evid,tvalue,store in postings[:limit]:
            if not IEventStore.providedBy(store):
                raise TypeError("store does not implement IEventStore")
            event = store.getEvent(evid)
            # keep a record of all field names found in the results.
            for fieldname in event.keys():
                if fieldname not in foundfields: foundfields.append(fieldname)
            # filter out unwanted fields
            if fields != None:
                event = dict([(k,v) for k,v in event.items() if k in fields])
            results.append((str(evid), event))
        return results, foundfields
    except Exception, e:
        logger.exception(e)
        raise
    # free any resources the searchers may be holding
    finally:
        for searcher in searchers: searcher.close()
