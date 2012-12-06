# Copyright 2010,2011 Michael Frank <msfrank@syntaxjockey.com>
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

import math
from zope.interface import implements
from terane.bier import ISearcher, IPostingList, IEventStore
from terane.bier.evid import EVID
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.searching')

class IndexSearcher(object):
    """
    IndexSearcher searches an entire index by searching each Segment individually
    and merging the results.
    """

    implements(ISearcher)

    def __init__(self, ix):
        """
        :param ix: The index to search.
        :type ix: :class:`terane.outputs.store.index.Index`
        """
        txn = ix.new_txn(TXN_SNAPSHOT=True)
        self._segmentSearchers = [SegmentSearcher(s,txn) for s in ix._segments]
        self._txn = txn

    def postingsLength(self, field, term, startId, endId):
        """
        Returns an estimate of the number of postings in the index within the
        specified period.

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
        length = 0
        for searcher in self._segmentSearchers:
            length += searcher.postingsLength(field, term, startId, endId)
        return length

    def postingsLengthBetween(self, field, startTerm, endTerm, startId, endId):
        length = 0
        for searcher in self._segmentSearchers:
            length += searcher.postingsLengthBetween(field, startTerm, endTerm, startId, endId)
        return length

    def iterPostings(self, field, term, startId, endId):
        """
        Returns a MergedPostingList which yields postings for the term in the
        specified field.  As a special case, if fieldname and term are None,
        then yield postings for all terms in all fields within the specified
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
        iters = [s.iterPostings(field, term, startId, endId)
                 for s in self._segmentSearchers]
        if endId < startId:
            compar = lambda d1,d2: cmp(d2,d1)
        else:
            compar = cmp
        return MergedPostingList(iters, compar)

    def iterPostingsBetween(self, field, startTerm, endTerm, startId, endId):
        iters = [s.iterPostingsBetween(field, startTerm, endTerm, startId, endId)
                 for s in self._segmentSearchers]
        if endId < startId:
            compar = lambda d1,d2: cmp(d2,d1)
        else:
            compar = cmp
        return MergedPostingList(iters, compar)

    def close(self):
        """
        Close the ISearcher, freeing any held resources.
        """
        for searcher in self._segmentSearchers: searcher.close()
        self._segmentSearchers = None
        self._txn.abort()
        self._txn = None

class MergedPostingList(object):
    """
    MergedPostingList iterates through a sequence of PostingList instances,
    merging the results in chronological order.
    """

    implements(IPostingList)

    def __init__(self, iters, compar):
        """
        :param iters: A sequence of :class:`terane.outputs.store.searching.PostingList` objects.
        :type iters: list
        """
        self._iters = iters
        self._cmp = compar
        self._smallestPostings = [(None,None,None) for i in range(len(iters))]
        self._lastId = None

    def nextPosting(self):
        """
        Returns the next posting, or None if iteration is finished.

        :returns: The next posting, which is a tuple containing the evid, the
          term value, and the searcher, or (None,None,None)
        :rtype: tuple
        """
        curr = 0
        # check each child iter for the lowest evid
        for i in range(len(self._iters)):
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
        return posting

    def skipPosting(self, targetId):
        """
        Skips to the targetId, returning the posting or None if the posting
        doesn't exist.

        :param targetId: The target evid to skip to.
        :type targetId: :class:`terane.bier.evid.EVID`
        :returns: The target posting, which is a tuple containing the evid,
          the term value, and the searcher, or (None,None,None)
        :rtype: tuple
        """
        posting = None
        # iterate through each child query
        for i in range(len(self._iters)):
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
        return posting

    def close(self):
        """
        Close the IPostingList, freeing any held resources.
        """
        for i in self._iters: i.close()
        self._iters = None
        self._smallestPostings = None
        self._lastId = None

class SegmentSearcher(object):
    """
    SegmentSearcher searches a single Segment.
    """

    implements(ISearcher, IEventStore)

    def __init__(self, segment, txn):
        """
        :param segment: The segment to search.
        :type segment: :class:`terane.outputs.store.segment.Segment`
        """
        self._segment = segment
        self._txn = txn 

    def postingsLength(self, field, term, startId, endId):
        """
        Returns an estimate of the number of postings in the segment within the
        specified period.

        :param field: The field to search within.
        :type field: :class:`terane.bier.fields.QualifiedField`
        :param term: The term to search for.
        :type term: object
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: An estimate of the number of postings.
        :rtype: int
        """
        try:
            if field == None and term == None:
                lastUpdate = self._segment.get_meta(self._txn, u'last-update')
                numDocs = lastUpdate[u'size']
                # startId may be greater than endId, but the estimate_events
                # method doesn't care
                start = [startId.ts, startId.offset]
                end = [endId.ts, endId.offset]
                estimate = self._segment.estimate_events(self._txn, start, end)
            else:
                start = [field.fieldname, field.fieldtype, term, startId.ts, startId.offset]
                end = [field.fieldname, field.fieldtype, term, endId.ts, endId.offset]
                field = self._segment.get_field(self._txn, [field.fieldname,field.fieldtype])
                numDocs = field[u'num-docs']
                # startId may be greater than endId, but the estimate_postings
                # method doesn't care
                estimate = self._segment.estimate_postings(self._txn, start, end)
            return int(math.ceil(numDocs * estimate))
        except KeyError:
            return 0

    def postingsLengthBetween(self, field, startTerm, endTerm, startId, endId):
        """
        """
        length = 0
        if startTerm == None:
            startKey = None
        else:
            startKey = [field.fieldname, field.fieldtype, startTerm]
        if endTerm == None:
            endKey = None
        else:
            endKey = [field.fieldname, field.fieldtype, endTerm]
        terms = self._segment.iter_terms(self._txn, startKey, endKey)
        for t in terms:
            length += self.postingsLength(field, t, startId, endId)
        terms.close()
        return length

    def iterPostings(self, field, term, startId, endId):
        """
        Returns a PostingList which yields postings for the term in the
        specified field.  As a special case, if fieldname and term are None,
        then yield postings for all terms in all fields within the specified
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
        if field == None and term == None:
            start = [startId.ts, startId.offset]
            end = [endId.ts, endId.offset]
            postings = self._segment.iter_events(self._txn, start, end)
        else:
            start = [field.fieldname, field.fieldtype, term, startId.ts, startId.offset]
            end = [field.fieldname, field.fieldtype, term, endId.ts, endId.offset]
            postings = self._segment.iter_postings(self._txn, start, end)
        return PostingList(self, field, term, postings)

    def iterPostingsBetween(self, field, startTerm, endTerm, startId, endId):
        """
        """
        # get an iterator yielding the terms
        if startTerm == None:
            startKey = None
        else:
            startKey = [field.fieldname, field.fieldtype, startTerm]
        if endTerm == None:
            endKey = None
        else:
            endKey = [field.fieldname, field.fieldtype, endTerm]
        terms = self._segment.iter_terms(self._txn, startKey, endKey)
        # get an iterator yielding the postings
        if startTerm == None:
            startKey = None
        else:
            startKey = [field.fieldname, field.fieldtype, startTerm, startId.ts, startId.offset]
        if endTerm == None:
            endKey = None
        else:
            endKey = [field.fieldname, field.fieldtype, endTerm, endId.ts, endId.offset]
        postings = self._segment.iter_postings(self._txn, startKey, endKey)
        # return posting list
        return MultiTermPostingList(self, field, terms, postings, startId, endId)

    def getEvent(self, evid):
        """
        Returns the event specified by evid.

        :param evid: The event identifier
        :type evid: :class:`terane.bier.evid.EVID`
        :returns: A dict mapping fieldnames to values.
        :rtype: dict
        """
        fields = self._segment.get_event(self._txn, [evid.ts, evid.offset])
        defaultfield = u'message'
        defaultvalue = fields[defaultfield]
        del fields[defaultfield]
        return defaultfield, defaultvalue, fields

    def close(self):
        """
        Close the ISearcher, freeing any held resources.
        """
        self._txn = None

class PostingList(object):
    """
    PostingList iterates through postings in chronological order.
    """
    
    implements(IPostingList)

    def __init__(self, searcher, field, term, postings):
        """
        :param searcher:
        :type searcher: :class:`terane.outputs.store.searching.SegmentSearcher`
        :param postings:
        :type postings: :class:`terane.outputs.store.backend.Iter`
        """
        self._searcher = searcher
        self._field = field
        self._term = term
        self._postings = postings
    
    def nextPosting(self):
        """
        Returns the next posting, or None if iteration is finished.

        :returns: The next posting, which is a tuple containing the evid, the
          term value, and the searcher, or (None,None,None)
        :rtype: tuple
        """
        if self._postings == None:
            return None, None, None
        try:
            key,value = self._postings.next()
            # posting key consists of: fieldname, fieldtype, term, ts, id
            if len(key) == 5:
                evid = EVID(key[3], key[4])
            # term key consists of: ts, id
            else:
                evid = EVID(key[0], key[1])
            return evid, value, self._searcher
        except StopIteration:
            self._postings.close()
            self._postings = None
        return None, None, None

    def skipPosting(self, targetId):
        """
        Skips to the targetId, returning the posting or None if the posting
        doesn't exist.

        :param targetId: The target evid to skip to.
        :type targetId: :class:`terane.bier.evid.EVID`
        :returns: The target posting, which is a tuple containing the evid,
          the term value, and the searcher, or (None,None,None)
        :rtype: tuple
        """
        if self._postings == None:
            return None, None, None
        try:
            target = [
                self._field.fieldname,
                self._field.fieldtype,
                self._term,
                targetId.ts,
                targetId.offset
                ]
            key,value = self._postings.skip(target)
            # posting key consists of: fieldname, fieldtype, term, ts, id
            if len(key) == 5:
                evid = EVID(key[3], key[4])
            # term key consists of: ts, id
            else:
                evid = EVID(key[0], key[1])
            return evid, value, self._searcher
        except IndexError:
            return None, None, None
        except StopIteration:
            self._postings.close()
            self._postings = None
        return None, None, None

    def close(self):
        """
        Close the IPostingList, freeing any held resources.
        """
        if not self._postings == None:
            self._postings.close()
        self._postings = None
        self._searcher = None

class MultiTermPostingList(object):

    implements(IPostingList)

    def __init__(self, searcher, field, terms, postings, startId, endId):
        """
        :param searcher:
        :type searcher: :class:`terane.outputs.store.searching.SegmentSearcher`
        :param field:
        :type field:
        :param terms:
        :type terms: :class:`terane.outputs.store.backend.Iter`
        :param postings:
        :type postings: :class:`terane.outputs.store.backend.Iter`
        """
        self._searcher = searcher
        self._field = field
        self._terms = terms
        self._postings = postings
        self._startId = startId
        self._endId = endId
        self._lastId = None
        if startId > endId:
            self._cmp = lambda x,y: cmp(y, x)
        else:
            self._cmp = cmp

    def nextPosting(self):
        """
        Returns the next posting, or None if iteration is finished.

        :returns: The next posting, which is a tuple containing the evid, the
          term value, and the searcher, or (None,None,None)
        :rtype: tuple
        """
        smallestId = None
        nextPosting = (None, None, None)
        # find the next posting closest to the smallestId
        for termKey,termValue in self._terms:
            if self._lastId == None:
                closestKey = termKey + [self._startId.ts, self._startId.offset] 
            else:
                closestKey = termKey + [self._lastId.ts, self._lastId.offset + 1] 
            try:
                postingKey,postingValue = self._postings.skip(closestKey, True)
            except IndexError:
                continue
            currId = EVID(postingKey[3], postingKey[4])
            #
            if ((smallestId == None or self._cmp(currId, smallestId) <= 0) and
              termKey == postingKey[0:3] and
              self._cmp(currId, self._endId) <= 0):
                smallestId = currId
                nextPosting = (smallestId, postingValue, self._searcher)
        # reset iterators if necessary
        if nextPosting != (None, None, None):
            self._terms.reset()
            self._postings.reset()
        # if the lowest id is greater than the endId, then we are done
        if smallestId == None or self._cmp(smallestId, self._endId) > 0:
            return (None, None, None)
        self._lastId = smallestId
        return nextPosting

    def skipPosting(self, targetId):
        """
        Skips to the targetId, returning the posting or None if the posting
        doesn't exist.

        :param targetId: The target evid to skip to.
        :type targetId: :class:`terane.bier.evid.EVID`
        :returns: The target posting, which is a tuple containing the evid,
          the term value, and the searcher, or (None,None,None)
        :rtype: tuple
        """
        return None, None, None

    def close(self):
        """
        Close the IPostingList, freeing any held resources.
        """
        if not self._terms == None:
            self._terms.close()
        if not self._postings == None:
            self._postings.close()
        self._terms = None
        self._postings = None
        self._searcher = None
