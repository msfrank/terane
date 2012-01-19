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
from terane.bier.docid import DocID
from terane.outputs.store.encoding import json_encode, json_decode
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
        self._segmentSearchers = [SegmentSearcher(s) for s in ix._segments]

    def postingsLength(self, fieldname, term, period):
        """
        Returns an estimate of the number of postings in the index within the
        specified period.

        :param fieldname: The name of the field to search within.
        :type fieldname: str
        :param term: The term to search for.
        :type term: unicode
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: An estimate of the number of postings.
        :rtype: int
        """
        length = 0
        for searcher in self._segmentSearchers:
            length += searcher.postingsLength(fieldname, term, period)
        return length

    def iterPostings(self, fieldname, term, period, reverse):
        """
        Returns a MergedPostingList which yields postings for the term
        in the specified field.

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
        iters = [s.iterPostings(fieldname, term, period, reverse) for s in self._segmentSearchers]
        return MergedPostingList(iters)

class MergedPostingList(object):
    """
    MergedPostingList iterates through a sequence of PostingList instances,
    merging the results in chronological order.
    """

    implements(IPostingList)

    def __init__(self, iters):
        """
        :param iters: A sequence of :class:`terane.outputs.store.searching.PostingList` objects.
        :type iters: list
        """
        self._iters = iters
        self._smallestPostings = [(None,None,None) for i in range(len(iters))]
        self._lastId = None

    def nextPosting(self):
        """
        Returns the next posting, or None if iteration is finished.

        :returns: The next posting, which is a tuple containing the docId, the term value, and the searcher, or (None,None,None)
        :rtype: tuple
        """
        curr = 0
        # check each child iter for the lowest docId
        for i in range(len(self._iters)):
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
            if self._smallestPostings[curr][0] == None or self._smallestPostings[i][0] < self._smallestPostings[curr][0]:
                curr = i
        # update lastId with the docId
        posting = self._smallestPostings[curr]
        self._lastId = posting[0]
        # forget the docId so we don't return it again
        self._smallestPostings[curr] = (None,None,None)
        return posting

    def skipPosting(self, targetId):
        """
        Skips to the targetId, returning the posting or None if the posting doesn't exist.

        :param targetId: The target docId to skip to.
        :type targetId: :class:`terane.bier.docid.DocID`
        :returns: The target posting, which is a tuple containing the docId, the term value, and the searcher, or (None,None,None)
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
            if posting[0] == None or posting[0] < targetId:
                posting = self._iters[i].skipPosting(targetId)
                self._smallestPostings[i] = posting
                if posting[0] == targetId:
                    break    
        return posting

class SegmentSearcher(object):
    """
    SegmentSearcher searches a single Segment.
    """

    implements(ISearcher, IEventStore)

    def __init__(self, segment):
        """
        :param segment: The segment to search.
        :type segment: :class:`terane.outputs.store.segment.Segment`
        """
        self._segment = segment

    def postingsLength(self, fieldname, term, period):
        """
        Returns an estimate of the number of postings in the segment within the
        specified period.

        :param fieldname: The name of the field to search within.
        :type fieldname: str
        :param term: The term to search for.
        :type term: unicode
        :param period: The period within which the search query is constrained.
        :type period: :class:`terane.bier.searching.Period`
        :returns: An estimate of the number of postings.
        :rtype: int
        """
        fieldname = str(fieldname)
        term = unicode(term)
        if period == None:
            try:
                tmeta = json_decode(self._segment.get_term_meta(None, fieldname, term))
                return tmeta['num-docs']
            except KeyError:
                return 0
        else:
            try:
                fmeta = json_decode(self._segment.get_field_meta(None, fieldname))
                ndocs = fmeta['num-docs']
                start = str(period.startingID())
                end = str(period.endingID())
                return int(math.ceil(ndocs * self._segment.estimate_term_postings(None, fieldname, term, start, end)))
            except KeyError:
                return 0
        
    def iterPostings(self, fieldname, term, period, reverse):
        """
        Returns a PostingList which yields postings for the term in the specified field.

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
        fieldname = str(fieldname)
        term = unicode(term)
        start = str(period.startingID())
        end = str(period.endingID())
        return PostingList(self, self._segment.iter_terms_within(None, fieldname, term, start, end, reverse))

    def getEvent(self, docId):
        """
        Returns the event specified by docId.

        :param docId: The event docId
        :type docId: :class:`terane.bier.docid.DocID`
        :returns: A dict mapping fieldnames to values.
        :rtype: dict
        """
        return json_decode(self._segment.get_doc(None, str(docId)))

class PostingList(object):
    """
    PostingList iterates through postings in chronological order.
    """
    
    implements(IPostingList)

    def __init__(self, searcher, postings):
        """
        :param searcher:
        :type searcher: :class:`terane.outputs.store.searching.SegmentSearcher`
        :param postings:
        :type postings: :class:`terane.outputs.store.backend.Iter`
        """
        self._searcher = searcher
        self._postings = postings
    
    def nextPosting(self):
        """
        Returns the next posting, or None if iteration is finished.

        :returns: The next posting, which is a tuple containing the docId, the term value, and the searcher, or (None,None,None)
        :rtype: tuple
        """
        if self._postings == None:
            return None, None, None
        try:
            docId,tvalue = self._postings.next()
            docId = DocID.fromString(docId)
            if tvalue == '':
                tvalue = None
            else:
                tvalue = json_decode(tvalue)
            return docId, tvalue, self._searcher
        except StopIteration:
            self._postings = None
        return None, None, None

    def skipPosting(self, targetId):
        """
        Skips to the targetId, returning the posting or None if the posting doesn't exist.

        :param targetId: The target docId to skip to.
        :type targetId: :class:`terane.bier.docid.DocID`
        :returns: The target posting, which is a tuple containing the docId, the term value, and the searcher, or (None,None,None)
        :rtype: tuple
        """
        if self._postings == None:
            return None, None, None
        try:
            targetId = str(targetId)
            docId,tvalue = self._postings.skip(targetId)
            docId = DocID.fromString(docId)
            if tvalue == '':
                tvalue = None
            else:
                tvalue = json_decode(tvalue)
            return docId, tvalue, self._searcher
        except IndexError:
            return None, None, None
        except StopIteration:
            self._postings = None
        return None, None, None
