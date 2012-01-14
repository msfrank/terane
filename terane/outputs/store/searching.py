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
from terane.bier.searching import ISearcher, IPostingList
from terane.bier.docid import DocID
from terane.outputs.store.encoding import json_encode, json_decode
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.searching')

class IndexSearcher(object):

    implements(ISearcher)

    def __init__(self, segment):
        self._segment = segment

    def __enter__(self):
        pass
    
    def postingsLength(self, fieldname, term, period):
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
        fieldname = str(fieldname)
        term = unicode(term)
        start = str(period.startingID())
        end = str(period.endingID())
        return PostingList(self._segment.iter_terms_within(None, fieldname, term, start, end, reverse))

    def getEvent(self, docId):
        return json_decode(self._segment.get_doc(None, str(docId)))

    def __exit__(self, excType, excValue, traceback):
        pass
    
class PostingList(object):
    
    implements(IPostingList)

    def __init__(self, postings):
        self._postings = postings
    
    def nextPosting(self):
        if self._postings == None:
            return None, None
        try:
            docId,tvalue = self._postings.next()
            docId = DocID.fromString(docId)
            if tvalue == '':
                tvalue = None
            else:
                tvalue = json_decode(tvalue)
            return docId, tvalue
        except StopIteration:
            self._postings = None
            return None, None

    def skipPosting(self, targetId):
        if self._postings == None:
            return None, None
        try:
            targetId = str(targetId)
            docId,tvalue = self._postings.skip(targetId)
            docId = DocID.fromString(docId)
            if tvalue == '':
                tvalue = None
            else:
                tvalue = json_decode(tvalue)
            return docId, tvalue
        except IndexError:
            return None, None
        except StopIteration:
            self._postings = None
            return None, None
