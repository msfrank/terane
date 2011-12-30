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

from terane.bier.searching import ISearcher, IPostingList
from terane.outputs.store.encoding import json_encode, json_decode
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.searching')

class IndexSearcher(object):

    implements(ISearcher)

    def __init__(self, segment):
        self._segment = segment

    def __enter__(self):
        pass
    
    def postingsLength(self, fieldname, term):
        logger.trace("SegmentSearcher.postingsLength(fieldname=%s, term=%s)" %
            (fieldname, term))
        fieldname = str(fieldname)
        term = unicode(term)
        try:
            tmeta = json_decode(self._segment.get_term_meta(None, fieldname, term))
            return tmeta['num-docs']
        except KeyError:
            return 0

    def iterPostings(self, fieldname, term, reverse):
        logger.trace("SegmentSearcher.iterPostings(fieldname=%s, term=%s, reverse=%s)" %
            (fieldname, term, reverse))
        fieldname = str(fieldname)
        term = unicode(term)
        return PostingList(self._segment.iter_terms(fieldname, term))

    def __exit__(self, excType, excValue, traceback):
        pass
    
class PostingList(object):
    
    implements(IPostingList)

    def __init__(self, postings):
        self._postings = postings
    
    def nextPosting(self):
        if self._postings == None:
            raise StopIteration()
        try:
            docId,tvalue = self._postings.next()
            docId = DocID.fromString(docId)
            tvalue = json_decode(tvalue)
            return docId, tvalue
        finally:
            self._postings = None

    def skipPosting(self, targetId):
        if self._postings == None:
            raise StopIteration()
        try:
            targetId = str(targetId)
            docId,tvalue = self._postings.skip(targetId)
            docId = DocID.fromString(docId)
            tvalue = json_decode(tvalue)
            return docId, tvalue
        finally:
            self._postings = None
