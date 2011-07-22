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
#
# ----------------------------------------------------------------------
#
# This file contains portions of code Copyright 2009 Matt Chaput
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from time import clock as now
from json import JSONDecoder
from heapq import nlargest, nsmallest
from bisect import bisect_right
from whoosh.reading import IndexReader as WhooshIndexReader
from whoosh.reading import MultiReader as WhooshMultiReader
from whoosh.matching import Matcher as WhooshMatcher, ReadTooFar
from terane.loggers import getLogger

logger = getLogger('terane.db.reading')

decoder = JSONDecoder()
def json_decode(u):
    # return the specified JSON string as a python object
    return decoder.decode(u)

class SegmentReader(WhooshIndexReader):
    def __init__(self, ix, segment):
        self._segment = segment
        self._schema = ix.schema
        
    @property
    def schema(self):
        """
        Return the schema from the TOC.
        """
        return self._schema

    def __contains__(self, term):
        # returns True if the field contains the specified word
        logger.debug("IndexReader.__contains__(fieldname=%s, word=%s)" % term)
        fieldname = str(term[0])
        word = unicode(term[1])
        return self._segment.contains_word(None, fieldname, word)
    
    def close(self):
        pass
    
    def has_deletions(self):
        # should always return False, since we don't batch up deletions
        return False
    
    def is_deleted(self, doc_id):
        # returns true if the document id doesn't exist in the store
        logger.debug("IndexReader.is_deleted(doc_id=%s)" % doc_id)
        doc_id = long(doc_id)
        if self._segment.contains_doc(None, doc_id) == True:
            return False
        return True

    def stored_fields(self, doc_id):
        # return a dict of fieldname:fieldvalue pairs for the specified document
        logger.debug("IndexReader.stored_fields(doc_id=%s)" % doc_id)
        doc_id = long(doc_id)
        doc = json_decode(self._segment.get_doc(None, doc_id))
        storedfields = {}
        for fieldname in self._schema.stored_names():
            if fieldname in doc['fields']:
                storedfields[fieldname] = doc['fields'][fieldname]
        return storedfields

    def all_stored_fields(self):
        # iterate through all documents, yielding a dict of fieldname:fieldvalue
        # pairs for each document.
        logger.debug("IndexReader.all_stored_fields()")
        for id,doc in self._segment.iter_docs(None):
            doc = json_decode(doc)
            storedfields = {}
            for fieldname in self._schema.stored_names():
                if fieldname in doc['fields']:
                    storedfields[fieldname] = doc['fields'][fieldname]
            yield storedfields

    def doc_count_all(self):
        # return the total number of documents in the store.
        logger.debug("IndexReader.doc_count_all()")
        return self._segment.count_docs()
    
    def doc_count(self):
        # return the total number of documents in the store.  This returns the
        # same value as doc_count_all().  the difference is that doc_count()
        # returns the number of undeleted documents, while doc_count_all()
        # returns the total number of documents.  since we don't keep track of
        # deleted documents (we just delete em :) these two values are always the
        # same.
        logger.debug("IndexReader.doc_count()")
        return self._segment.count_docs()
    
    def field_length(self, fieldname):
        # return the total number of terms in the specified field. 
        logger.debug("IndexReader.field_length(fieldname=%s)" % fieldname)
        fieldname = str(fieldname)
        fmeta = self._segment.get_field_meta(None, fieldname)
        if fmeta == None:
            return 0
        fmeta = json_decode(fmeta)
        try:
            return fmeta['fieldlength']
        except:
            return 0
    
    def max_field_length(self, fieldname):
        # return the maximum termlength in the specified field. 
        logger.debug("IndexReader.max_field_length(fieldname=%s)" % fieldname)
        fieldname = str(fieldname)
        fmeta = self._segment.get_field_meta(None, fieldname)
        if fmeta == None:
            return 0
        fmeta = json_decode(fmeta)
        try:
            return fmeta['maxfreq']
        except:
            return 0

    def doc_field_length(self, doc_id, fieldname):
        # return the number of terms in the specified field for the
        # specified document.
        logger.debug("IndexReader.doc_field_length(doc_id=%s, fieldname=%s)" %
            (doc_id,fieldname))
        doc_id = long(doc_id)
        fieldname = str(fieldname)
        try:
            doc = json_decode(self._segment.get_doc(None, doc_id))
            return doc['fields'][fieldname]['fieldlength']
        except KeyError:
            return 0
        except IndexError:
            return 0
    
    def has_vector(self, doc_id, fieldname):
        # return True if the specified document has vectors for the specified
        # field.
        logger.debug("IndexReader.has_vector(doc_id=%s, fieldname=%s)" %
            (doc_id,fieldname))
        doc_id = long(doc_id)
        fieldname = str(fieldname)
        try:
            doc = json_decode(self._segment.get_doc(None, doc_id))
        except IndexError:
            return False
        if 'vectors' in doc['fields'][fieldname]:
            return True
        return False
    
    def vector(self, doc_id, fieldname):
        # return document vector for the specified field of the specified document.
        logger.debug("IndexReader.vector(doc_id=%s, fieldname=%s)" %
            (doc_id,fieldname))
        doc_id = long(doc_id)
        fieldname = str(fieldname)
        doc = json_decode(self._segment.get_doc(None, doc_id))
        vformat = self._schema[fieldname].vector
        return PostingReader(vformat, doc['fields'][fieldname]['vectors'])

    def __iter__(self):
        # iterate through the index, yielding tuples consisting of
        # (fieldname, text, document count, word frequency)
        logger.debug("IndexReader.__iter__()")
        for fieldname in self._schema.names():
            for word,wmeta in self._segment.iter_words_meta(None, fieldname):
                wmeta = json_decode(wmeta)
                yield (fieldname, word, wmeta['ndocs'], wmeta['freq'])
                
    def doc_frequency(self, fieldname, word):
        # return the amount of documents in the specified field which contain
        # the specified word.
        logger.debug("IndexReader.doc_frequency(fieldname=%s, word=%s)" %
            (fieldname, word))
        fieldname = str(fieldname)
        word = unicode(word)
        try:
            wmeta = json_decode(self._segment.get_word_meta(None, fieldname, word))
            return wmeta['ndocs']
        except KeyError:
            return 0

    def frequency(self, fieldname, word):
        # return the frequency of the specified word in the specified field.
        logger.debug("IndexReader.frequency(fieldname=%s, word=%s)" %
            (fieldname, word))
        fieldname = str(fieldname)
        word = unicode(word)
        try:
            wmeta = json_decode(self._segment.get_word_meta(None, fieldname, word))
            return wmeta['freq']
        except KeyError:
            return 0
    
    def iter_from(self, fieldname, start):
        # iterate through the index, yielding tuples consisting of
        # (field number, text, document count, word frequency), starting
        # at the specified text.
        logger.debug("IndexReader.iter_from(fieldname=%s, start=%s)" %
            (fieldname, start))
        fieldname = str(fieldname)
        start = unicode(start)
        for fn in (fn for fn in self._schema.names() if fn >= fieldname):
            if fn == fieldname:
                for word,wmeta in self._segment.iter_words_meta_from(None, fn, start):
                    wmeta = json_decode(wmeta)
                    yield (fieldname, word, wmeta['ndocs'], wmeta['freq'])
            else:
                for word,wmeta in self._segment.iter_words_meta(None, fn):
                    wmeta = json_decode(wmeta)
                    yield (fieldname, word, wmeta['ndocs'], wmeta['freq'])
    
    def lexicon(self, fieldname):
        # return a sorted list of the indexed terms for the specified field.
        logger.debug("IndexReader.lexicon(fieldname=%s)" % fieldname)
        fieldname = str(fieldname)
        return [word for word,unused in self._segment.iter_words_meta(None, fieldname)]
    
    def iter_field(self, fieldname, prefix=u''):
        # iterate through the index for the specified field, yielding tuples
        # consisting of (text, document count, word frequency), starting
        # at the specified prefix.
        logger.debug("IndexReader.iter_field(fieldname=%s, prefix=%s)" %
            (fieldname, prefix))
        fieldname = str(fieldname)
        prefix = unicode(prefix)
        for word,wmeta in self._segment.iter_words_meta_from(None, fieldname, prefix):
            wmeta = json_decode(wmeta)
            yield (word, wmeta['ndocs'], wmeta['freq'])
         
    def expand_prefix(self, fieldname, prefix):
        # iterate through the the specified field, yielding terms which start
        # with the specified prefix.
        logger.debug("IndexReader.expand_prefix(fieldname=%s, prefix=%s)" %
            (fieldname,prefix))
        fieldname = str(fieldname)
        prefix = unicode(prefix)
        try:
            for word,wmeta in self._segment.iter_words_meta_range(None, fieldname, prefix):
                yield word
        except KeyError:
            return
            
    def postings(self, fieldname, word, scorer=None):
        # return a PostingReader which contains a list of all the document ids
        # and their corresponding value strings for the specified word in the
        # specified field, excluding any document ids listed in exclude_docs.
        logger.debug("IndexReader.postings(fieldname=%s, word=%s, scorer=%s)" %
            (fieldname, word, scorer))
        fieldname = str(fieldname)
        word = unicode(word)
        postings = self._segment.iter_words(None, fieldname, word)
        format = self._schema[fieldname].format
        return PostingReader(word, format, postings, scorer)

    def supports_caches(self):
        # we don't support field caching yet.
        return False
                                   
    def _keyfn(self, fieldnames):
        def _keyfnwrapper(doc_id):
            stored = self.stored_fields(doc_id)
            return tuple(v for k,v in stored.items() if k in fieldnames)
        return _keyfnwrapper

    def sort_docs_by(self, fieldnames, doc_ids, reverse=False):
        # Returns a version of `docnums` sorted by the value of a field
        # in each document.
        logger.debug("IndexReader.sort_docs_by(fieldnames=%s, doc_ids=%s, reverse=%s)" %
            (fieldnames, str(doc_ids), reverse))
        if isinstance(fieldnames, str):
            fieldnames = (fieldnames,)
        # special case: if fieldname is 'id', then sort doc_ids numerically
        if fieldnames == ('id',):
            return sorted(doc_ids, reverse=reverse)
        return sorted(doc_ids, key=self._keyfn(fieldnames), reverse=reverse)
 
    def key_docs_by(self, fieldnames, doc_ids, limit, reverse=False, offset=0):
        # Returns a sequence of `(sorting_key, docnum)` pairs for the
        # document numbers in `docnum`.
        logger.debug("IndexReader.key_docs_by(fieldnames=%s, doc_ids=%s, limit=%i, reverse=%s, offset=%i)" %
            (fieldnames, str(doc_ids), limit, reverse, offset))
        if isinstance(fieldnames, str):
            fieldnames = (fieldnames,)
        keyfn = self._keyfn(fieldnames)
        if limit is None:
            return [(keyfn(doc_id), doc_id) for doc_id in doc_ids]
        if reverse:
            return nlargest(limit, ((keyfn(doc_id), doc_id) for doc_id in doc_ids))
        return nsmallest(limit, ((keyfn(doc_id), doc_id) for doc_id in doc_ids))

class PostingReader(WhooshMatcher):
    def __init__(self, word, format, postings, scorer=None):
        self._word = word
        self._format = format
        self._postings = postings
        self._current = None
        self._scorer = scorer
        logger.debug("PostingReader.__init__(word=%s)" % self._word)
    
    def is_active(self):
        # return True if there are still items to iterate over, otherwise False
        return self._postings != None

    def _is_uninitialized(self):
        if self.is_active() and self._current == None:
            return True
        return False

    def next(self):
        try:
            # move to the next item in the iterator.
            doc_id,wvalue = self._postings.next()
            wvalue = json_decode(wvalue)
            self._current = (doc_id, wvalue['weight'], wvalue['value'])
            logger.debug("PostingReader.next(word=%s): doc_id=%s, weight=%s, value=%s" %
                (self._word, self._current[0], self._current[1], self._current[2]))
            return True
        except StopIteration:
            self._current = None
            self._postings = None
            return False

    def id(self):
        # return the value of the current iterator item.
        logger.debug("PostingReader.id(word=%s)" % self._word)
        if self._is_uninitialized(): self.next()
        return self._current[0]

    def weight(self):
        # return the value of the current iterator item.
        logger.debug("PostingReader.weight(word=%s)" % self._word)
        if self._is_uninitialized(): self.next()
        return self._current[1]

    def value(self):
        # return the value string of the current iterator item.
        logger.debug("PostingReader.value(word=%)" % self._word)
        if self._is_uninitialized(): self.next()
        return self._current[2]

    def all_items(self):
        # return a list of all (document id, value string) tuples.
        logger.debug("PostingReader.all_items(word=%s)" % self._word)
        self._postings.reset()
        while self.is_active():
            if not self.next(): return
            item = (self._current[0], self._current[2])
            logger.debug("PostingReader.all_items(word=%s) => (%s, %s)" %
                (self._word, item[0], item[1]))
            yield item
    
    def all_ids(self):
        # return a list of all document ids.
        logger.debug("PostingReader.all_ids(word=%s)" % self._word)
        self._postings.reset()
        while self.is_active():
            if not self.next(): return
            id = self._current[0]
            logger.debug("PostingReader.all_ids(word=%s) => %s" %
                (self._word, id))
            yield id
   
    def items_as(self, astype):
        # return a list of all (document id, value) tuples, with each value
        # coerced to the type specified by astype.
        logger.debug("PostingReader.items_as(word=%s, astype=%s)" %
            (self._word,astype))
        self._postings.reset()
        while self.is_active():
            if not self.next(): return
            item = (self.id(), self.value_as(astype))
            logger.debug("PostingReader.items_as(word=%s) => (%s, %s)" %
                (self._word, item[0], item[1]))
            yield item

    def skip_to(self, target_id):
        # skip ahead to the specified document id.
        target_id = long(target_id)
        logger.debug("PostingReader.skip_to(word=%s, target_id=%s)" %
            (self._word,target_id))
        if not self.is_active():
            raise ReadTooFar
        # if we are already up to or past the target_id, then return
        if target_id <= self._current[0]:
            return
        # move to the target item
        try:
            doc_id,wvalue = self._postings.skip(target_id)
        except IndexError:
            raise ReadTooFar
        wvalue = json_decode(wvalue)
        self._current = (doc_id, wvalue['weight'], wvalue['value'])
        logger.debug("PostingReader.skip_to(word=%s): jumped to doc_id=%s, weight=%s, value=%s" %
            (self._word, self._current[0], self._current[1], self._current[2]))

    def score(self):
        logger.debug("PostingReader.score(word=%s)" % self._word)
        return self._scorer.score(self)
    
    def quality(self):
        logger.debug("PostingReader.quality(word=%s)" % self._word)
        return self._scorer.quality(self)
    
    def block_quality(self):
        logger.debug("PostingReader.block_quality(word=%s)" % self._word)
        return self._scorer.block_quality(self)

class MultiReader(WhooshMultiReader):

    def __init__(self, ix):
        readers = [SegmentReader(ix, s) for s,_ in ix._segments]
        WhooshMultiReader.__init__(self, readers)
        self.schema = ix.schema
        offsets = []
        for r in self.readers:
            try:
                offsets.append((r._segment.last_doc(None)[0],r))
            except IndexError:
                offsets.append((0, r))
        offsets.sort(key=lambda x: x[0])
        self.doc_offsets, self.readers = zip(*offsets)
        logger.debug("MultiReader.__init__(): readers=%s, offsets=%s" % (self.readers,self.doc_offsets))

    def _document_segment(self, doc_id):
        return max(0, bisect_right(self.doc_offsets, doc_id - 1))

    def _segment_and_docnum(self, doc_id):
        segment = self._document_segment(doc_id)
        offset_doc = doc_id
        logger.debug("MultiReader._segment_and_docnum(doc_id=%i): segment is %i, offset_doc is %i" % (
            doc_id, segment, offset_doc))
        return segment, offset_doc

    def add_reader(self, reader):
        raise NotImplemented()
