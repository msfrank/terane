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
from twisted.internet.threads import deferToThread
from twisted.internet.defer import succeed, inlineCallbacks, returnValue
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
        self._ix = ix
        self._txn = txn = ix.new_txn(TXN_SNAPSHOT=True)
        self._segmentSearchers = [
            SegmentSearcher(s, txn) for s in ix._segments]

    def getField(self, fieldname, fieldtype):
        """
        Return the field specified by the fieldname and fieldtype.  If the
        field doesn't exist, return None.
        """
        def _getField(searcher, fieldname, fieldtype):
            ix = searcher._ix
            with ix._fieldLock:
                if not fieldname in ix._fields:
                    return None
                fieldspec = ix._fields[fieldname]
                if not fieldtype in fieldspec:
                    return None
                return fieldspec[fieldtype]
        return deferToThread(_getField, self, fieldname, fieldtype)
        
    @inlineCallbacks
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
        for searcher in self._searchers:
            length += (yield searcher.postingsLength(field, term, startId, endId))
        returnValue(length)

    @inlineCallbacks
    def postingsLengthBetween(self, field, startTerm, endTerm, startEx, endEx, startId, endId):
        """
        Returns an estimate of the number of possible postings for all the terms
        between startTerm and endTerm.  If startTerm is None, then start from the
        first term.  If endTerm is None, then end at the last term.  If startEx
        or endEx are True, then exclude the start or end terms, respectively.
        """
        length = 0
        for searcher in self._searchers:
            length += (yield searcher.postingsLengthBetween(field,
                startTerm, endTerm, startEx, endEx, startId, endId))
        returnValue(length)

    @inlineCallbacks
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
        iters = [
            (yield s.iterPostings(field, term, startId, endId))
            for s in self._segmentSearchers]
        if endId < startId:
            compar = lambda d1,d2: cmp(d2,d1)
        else:
            compar = cmp
        returnValue(MergedPostingList(iters, compar))

    @inlineCallbacks
    def iterPostingsBetween(self, field, startTerm, endTerm,
                            startEx, endEx, startId, endId):
        """
        Returns an object implementing IPostingList which yields postings for all
        of the terms between startTerm and endTerm.  If startTerm is None, then
        start from the first term.  If endTerm is None, then end at the last term.
        If startEx or endEx are True, then exclude the start or end terms, respectively.
        """
        iters = [
            (yield s.iterPostingsBetween(field, startTerm, endTerm,
                                          startEx, endEx, startId, endId))
            for s in self._segmentSearchers]
        if endId < startId:
            compar = lambda d1,d2: cmp(d2,d1)
        else:
            compar = cmp
        returnValue(MergedPostingList(iters, compar))

    def close(self):
        """
        Close the ISearcher, freeing any held resources.
        """
        for s in self._segmentSearchers:
            s._close()
        self._segmentSearchers = None
        self._txn.abort()
        self._txn = None
        return succeed(None)

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

    @inlineCallbacks
    def nextPosting(self):
        """
        Returns the next posting, or None if iteration is finished.
        
        :returns: The next posting, which is a tuple containing the evid, the
          term value, and the searcher, or (None,None,None)
        :rtype: tuple
        """
        posting = (None, None, None)
        curr = 0
        # check each child iter for the lowest evid
        for i in range(len(self._iters)):
            # if None, then get the next posting from the iter
            if self._smallestPostings[i][0] == None:
                self._smallestPostings[i] = yield self._iters[i].nextPosting()
            # if the posting evid is None, then check the next iter
            if self._smallestPostings[i][0] == None:
                continue
            # if the evid equals the last evid returned, then ignore it
            if (self._lastId != None and
              self._smallestPostings[i][0] == self._lastId):
                self._smallestPostings[i] = (None,None,None)
                continue
            # we don't compare the first evid with itself
            if i == 0:
                continue
            # if the evid is the current smallest evid, then remember it
            if (self._smallestPostings[curr][0] == None or
              self._cmp(self._smallestPostings[i][0],
              self._smallestPostings[curr][0]) < 0):
                curr = i
        # update lastId with the evid
        posting = self._smallestPostings[curr]
        self._lastId = posting[0]
        # forget the evid so we don't return it again
        self._smallestPostings[curr] = (None,None,None)
        returnValue(posting)

    @inlineCallbacks
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
        posting = (None, None, None)
        # iterate through each child query
        for i in range(len(self._iters)):
            posting = self._smallestPostings[i]
            # if the smallestId equals the targetId, we are done
            if posting[0] == self._targetId:
                break
            # otherwise check if the targetId exists in the child query
            if posting[0] == None or self._cmp(posting[0], self._targetId) < 0:
                posting = yield self._iters[i].skipPosting(self._targetId)
                self._smallestPostings[i] = posting
                if posting[0] == self._targetId:
                    break    
        returnValue(posting)

    @inlineCallbacks
    def close(self):
        """
        Close all child posting lists, freeing their resources. 
        """
        for i in self._iters:
            yield i.close()
        self._iters = None
        self._smallestPostings = None
        self._lastId = None

class SegmentSearcher(object):
    """
    SegmentSearcher searches a single Segment.
    """

    implements(IEventStore)

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
        return deferToThread(self._postingsLength, field, term, startId, endId)

    def _postingsLength(self, field, term, startId, endId):
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

    def postingsLengthBetween(self, field, startTerm, endTerm, startEx, endEx, startId, endId):
        """
        """
        def _postingsLengthBetween(searcher, field, startTerm, endTerm, startEx, endEx, startId, endId):
            length = 0
            startKey = None if startTerm == None else [field.fieldname, field.fieldtype, startTerm]
            endKey = None if endTerm == None else [field.fieldname, field.fieldtype, endTerm]
            terms = searcher._segment.iter_terms(searcher._txn, startKey, endKey, False)
            for termKey,termValue in terms:
                if startEx and termKey == startTerm:
                    continue
                if endEx and termKey == endTerm:
                    continue
                length += searcher._postingsLength(field, termKey[2], startId, endId)
            terms.close()
            return length
        return deferToThread(_postingsLengthBetween, self, field, startTerm,
            endTerm, startEx, endEx, startId, endId)

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
        def _iterPostings(searcher, field, term, startId, endId):
            if field == None and term == None:
                startKey = [startId.ts, startId.offset]
                endKey = [endId.ts, endId.offset]
                if startId > endId:
                    startKey, endKey = endKey, startKey
                postings = searcher._segment.iter_events(searcher._txn, startKey, endKey,
                    True if startId > endId else False)
            else:
                startKey = [field.fieldname, field.fieldtype, term, startId.ts, startId.offset]
                endKey = [field.fieldname, field.fieldtype, term, endId.ts, endId.offset]
                if startId > endId:
                    startKey, endKey = endKey, startKey
                postings = searcher._segment.iter_postings(self._txn, startKey, endKey,
                    True if startId > endId else False)
            return PostingList(searcher, field, term, postings)
        return deferToThread(_iterPostings, self, field, term, startId, endId)

    def iterPostingsBetween(self, field, startTerm, endTerm, startEx, endEx, startId, endId):
        """
        """
        def _iterPostingsBetween(searcher, field, startTerm, endTerm,
                                 startEx, endEx, startId, endId):
            # get an iterator yielding the terms
            startKey = [field.fieldname, field.fieldtype] if startTerm == None else \
                [field.fieldname, field.fieldtype, startTerm]
            endKey = None if endTerm == None else \
                [field.fieldname, field.fieldtype, endTerm]
            terms = searcher._segment.iter_terms(searcher._txn, startKey, endKey, False)
            # get an iterator yielding the postings
            startKey = None if startTerm == None else \
                [field.fieldname, field.fieldtype, startTerm, startId.ts, startId.offset]
            endKey = None if endTerm == None else \
                [field.fieldname, field.fieldtype, endTerm, endId.ts, endId.offset]
            postings = searcher._segment.iter_postings(searcher._txn, startKey, endKey,
                True if startId > endId else False)
            # return posting list
            startEx = startTerm if startEx == True else None
            endEx = endTerm if endEx == True else None
            return MultiTermPostingList(searcher, field, terms, startEx, endEx,
                                        postings, startId, endId)
        return deferToThread(_iterPostingsBetween, self, field, startTerm, endTerm,
                             startEx, endEx, startId, endId)

    def getEvent(self, evid):
        """
        Returns the event specified by evid.

        :param evid: The event identifier
        :type evid: :class:`terane.bier.evid.EVID`
        :returns: A dict mapping fieldnames to values.
        :rtype: dict
        """
        def _getEvent(searcher, evid):
            evid = [evid.ts, evid.offset]
            segment = searcher._segment
            fields = segment.get_event(searcher._txn, evid)
            defaultfield = u'message'
            defaultvalue = fields[defaultfield]
            del fields[defaultfield]
            return (defaultfield, defaultvalue, fields)
        return deferToThread(_getEvent, self, evid)

    def _close(self):
        """
        Release our reference to the Txn.
        """
        self._txn = None

    def close(self):
        """
        We don't do anything, the SegmentSearcher is closed later.
        """
        return succeed(None)

class PostingList(object):
    """
    PostingList iterates through postings in chronological order.
    """
    
    def __init__(self, searcher, field, term, postings):
        self._searcher = searcher
        self._field = field
        self._term = term
        self._postings = postings
    
    def nextPosting(self):
        """
        Returns the next posting, or (None,None,None) if iteration is finished.
        """
        def _nextPosting(postingList):
            posting = (None, None, None)
            if postingList._postings == None:
                return posting
            try:
                key,value = postingList._postings.next()
                # posting key consists of: fieldname, fieldtype, term, ts, id
                if len(key) == 5:
                    evid = EVID(key[3], key[4])
                # term key consists of: ts, id
                else:
                    evid = EVID(key[0], key[1])
                posting = (evid, value, postingList._searcher)
            except StopIteration:
                postingList._close()
            return posting
        return deferToThread(_nextPosting, self)

    def skipPosting(self, targetId):
        """
        Skips to the targetId, returning the posting or (None,None,None) if
        the posting doesn't exist.
        """
        def _skipPosting(postingList, targetId):
            posting = (None, None, None)
            if postingList._postings == None:
                return posting
            try:
                target = [
                    postingList._field.fieldname,
                    postingList._field.fieldtype,
                    postingList._term,
                    targetId.ts,
                    targetId.offset
                    ]
                key,value = postingList._postings.skip(target)
                # posting key consists of: fieldname, fieldtype, term, ts, id
                if len(key) == 5:
                    evid = EVID(key[3], key[4])
                # term key consists of: ts, id
                else:
                    evid = EVID(key[0], key[1])
                posting = (evid, value, postingList._searcher)
            except IndexError:
                pass
            except StopIteration:
                postingList._close()
            return posting
        return deferToThread(_skipPosting, self, targetId)

    def _close(self):
        """
        Close the PostingList, freeing any held resources.
        """
        if not self._postings == None:
            self._postings.close()
        self._postings = None
        self._searcher = None

    def close(self):
        return deferToThread(self._close)

class MultiTermPostingList(object):

    def __init__(self, searcher, field, terms, startEx, endEx, postings, startId, endId):
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
        self._startEx = startEx
        self._endEx = endEx
        self._postings = postings
        self._startId = startId
        self._endId = endId
        self._lastId = None
        self._reverse = True if startId > endId else False
        if self._reverse:
            self._cmp = lambda x,y: cmp(y,x)
        else:
            self._cmp = cmp

    def nextPosting(self):
        """
        Returns the next posting, or (None,None,None) if iteration
        is finished.
        """
        def _nextPosting(postingList):
            prefix = [postingList._field.fieldname, postingList._field.fieldtype]
            smallestId = None
            nextPosting = (None, None, None)
            # find the next posting closest to the smallestId
            for termKey,termValue in postingList._terms:
                # check whether we should exclude this term
                if postingList._startEx and postingList._startEx == termKey[2]:
                    continue
                if postingList._endEx and postingList._endEx == termKey[2]:
                    continue
                # check if we have iterated past the last term for the specified field
                if termKey[0:2] != prefix:
                    break
                # jump to the next closest key
                if postingList._lastId == None:
                    closestKey = termKey + [postingList._startId.ts, postingList._startId.offset] 
                else:
                    if postingList._reverse:
                        closestKey = termKey + [postingList._lastId.ts, postingList._lastId.offset - 1] 
                    else:
                        closestKey = termKey + [postingList._lastId.ts, postingList._lastId.offset + 1] 
                try:
                    postingKey,postingValue = postingList._postings.skip(closestKey, True)
                except IndexError:
                    continue
                logger.trace("next range posting: %s" % postingKey)
                currId = EVID(postingKey[3], postingKey[4])
                # check whether this posting is the smallest so far
                if ((smallestId == None or postingList._cmp(currId, smallestId) < 0) and
                  termKey == postingKey[0:3] and postingList._cmp(currId, postingList._endId) <= 0):
                    smallestId = currId
                    nextPosting = (smallestId, postingValue, postingList._searcher)
            # no more ids, we are done
            if smallestId == None:
                return (None, None, None)
            # rewind the iterators
            postingList._terms.reset()
            postingList._postings.reset()
            postingList._lastId = smallestId
            return nextPosting
        return deferToThread(_nextPosting, self)

    def skipPosting(self, targetId):
        """
        Skips to the targetId, returning the posting or (None,None,None)
        if the posting doesn't exist.
        """
        def _skipPosting(postingList, targetId):
            prefix = [postingList._field.fieldname, postingList._field.fieldtype]
            nextPosting = (None, None, None)
            # find the next posting closest to the smallestId
            for termKey,termValue in postingList._terms:
                # check whether we should exclude this term
                if postingList._startEx and postingList._startEx == termKey[2]:
                    continue
                if postingList._endEx and postingList._endEx == termKey[2]:
                    continue
                # check if we have iterated past the last term for the specified field
                if termKey[0:2] != prefix:
                    break
                # jump to the next closest key
                targetKey = termKey + [targetId.ts, targetId.offset]
                try:
                    postingKey,postingValue = postingList._postings.skip(targetKey, False)
                except IndexError:
                    continue
                logger.trace("skip range posting: %s" % postingKey)
                currId = EVID(postingKey[3], postingKey[4])
                nextPosting = (currId, postingValue, postingList._searcher)
            # rewind the iterators
            postingList._terms.reset()
            postingList._postings.reset()
            return nextPosting
        return deferToThread(_skipPosting, self, targetId)

    def _close(self):
        """
        Close the MultiTermPostingList, freeing any held resources.
        """
        if self._terms:
            self._terms.close()
        if self._postings:
            self._postings.close()
        self._terms = None
        self._postings = None
        self._searcher = None

    def close(self):
        return deferToThread(self._close)
