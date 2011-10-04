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

import pickle, time
from json import JSONEncoder
from whoosh.fields import UnknownFieldError
from whoosh.writing import IndexWriter as WhooshIndexWriter
from terane.outputs.store.reading import json_decode
from terane.outputs.store.storage import DocExists, Txn
from terane.loggers import getLogger

logger = getLogger('terane.db.writing')

encoder = JSONEncoder(False, False)
def json_encode(o):
    # return the specified object as a JSON-encoded unicode string
    return unicode(encoder.encode(o))

class WriterExpired(Exception):
    pass

class IndexWriter(WhooshIndexWriter):
    def __init__(self, ix):
        self._ix = ix
        self._segment = ix._current[0]
        self._txn = Txn(ix._env)

    def add_field(self, fieldname, fieldspec):
        raise Exception("IndexWriter.add_field() not implemented")

    def remove_field(self, fieldname):
        raise Exception("IndexWriter.remove_field() not implemented")

    def add_document(self, **fields):
        if not self._txn:
            raise WriterExpired("IndexWriter transaction was already commited")
        with Txn(self._ix._env, self._txn) as doc_txn:
            self._lastid = self._add(doc_txn, fields)
        self._ix._size += 1
        self._ix._lastmodified = int(time.time())

    def _add(self, doc_txn, fields):
        # create a list of valid field names from the passed in fields. a valid
        # field name is defined as any name that starts with an alphabetic character
        fieldnames = [fieldname for fieldname in fields.keys() + ['ts','id']
            if fieldname[0].isalpha()]
        
        # verify that each field name exists in the index schema
        for fieldname in fieldnames:
            if fieldname not in self._ix.schema:
                raise UnknownFieldError("There is no field named %r" % fieldname)
            
        # create a document record
        doc_id = self._ix._ids.allocate()
        self._segment.new_doc(doc_txn, doc_id)
        logger.trace("created new document with id %s" % doc_id)
        # add the document id to the fields, so we can search on it
        fields['id'] = long(doc_id)
            
        doc_fields = {}

        # for each field in the schema
        for fieldname in fieldnames:
            field = self._ix.schema[fieldname]
            doc_field = {}

            # create a transaction for each field in the document
            field_txn = Txn(self._ix._env, doc_txn)

            # if the field exists in the document, get its value
            value = fields.get(fieldname)

            if value:
                logger.trace("field=%s,doc=%s: raw_value='%s'" % (fieldname,doc_id,value))
                format = field.format
                count = 0

                # FIXME: is it correct to verify value is a unicode object?
                ## verify that textual field values are unicode
                #if format.analyzer:
                #    if format.textual and not isinstance(value, unicode):
                #        raise ValueError("%r in field %s is not unicode" % (value, fieldname))
                    
                # index the field value.  we iterate over a series of
                # (w,freq,valuestring) tuples, where w is the tokenized
                # term, freq is the frequency of w, and valuestring is
                # format-specific data about the posting.
                maxfreq = 0
                for word, freq, weight, valuestring in field.index(value):
                    word = unicode(word)
                    try:
                        wmeta = self._segment.get_word_meta(field_txn, fieldname, word)
                        # increment the term frequency and the document
                        # count for this field
                        wmeta = json_decode(wmeta)
                        if not 'freq' in wmeta:
                            wmeta['freq'] = freq
                        else:
                            wmeta['freq'] += freq
                        if not 'ndocs' in wmeta:
                            wmeta['ndocs'] = 1
                        else:
                            wmeta['ndocs'] += 1
                    except KeyError:
                        # if the word doesn't exist in the inverted index, then we save
                        # the term frequency as the first value for key w, and set the
                        # document count to 1.
                        wmeta = {'freq': freq, 'ndocs': 1}
                    # keep track of the maximum frequency to store in the field metadata
                    if wmeta['freq'] > maxfreq:
                        maxfreq = wmeta['freq']
                    wmeta = json_encode(wmeta)
                    logger.trace("field=%s,doc=%s,word=%s: word_meta=%s" % (fieldname,doc_id,word,wmeta))
                    self._segment.set_word_meta(field_txn, fieldname, word, wmeta)
                    # increment the total field frequency
                    count += freq
                    # add the valuestring and record number to the inverted index
                    # for the field.
                    wvalue = json_encode({'doc': doc_id, 'weight': weight, 'value': valuestring})
                    logger.trace("field=%s,doc=%s,word=%s: doc_word=%s" % (fieldname,doc_id,word,wvalue))
                    self._segment.set_word(field_txn, fieldname, word, doc_id, wvalue)

                # get the field metadata
                try:
                    fmeta = self._segment.get_field_meta(field_txn, fieldname)
                    fmeta = json_decode(fmeta)
                    if not 'fieldlength' in fmeta:
                        fmeta['fieldlength'] = 0
                    if not 'maxfreq' in fmeta:
                        fmeta['maxfreq'] = 0
                except KeyError:
                    fmeta = {'fieldlength': 0, 'maxfreq': 0}
                # if maxfreq is greater than the current field maxfreq, then
                # update the field metadata.
                if maxfreq > fmeta['maxfreq']:
                    fmeta['maxfreq'] = maxfreq
                # if the field is scorable, then we increment the fieldlength
                # for this record and the fieldlength total for the field by the
                # sum of the frequencies of the indexed field.
                if field.scorable:
                    if not 'fieldlength' in fmeta:
                        fmeta['fieldlength'] = count
                    else:
                        fmeta['fieldlength'] += count
                    doc_field['fieldlength'] = count
                # set the field metadata
                fmeta = json_encode(fmeta)
                logger.trace("field=%s: field_meta=%s" % (fieldname,fmeta))
                self._segment.set_field_meta(field_txn, fieldname, fmeta)
                
            # if the field has a vector defined, then generate a sorted
            # list of (term,valuestring) tuples from the field value and store
            # it.
            vector = field.vector
            if vector:
                doc_field['vectors'] = sorted((word, weight, valuestring)
                    for word,freq,weight,valuestring in vector.word_values(value))
            
            # if the full field value should be stored alongside the document,
            # then fill in the storedvalues array with the actual field value.
            # if the key '&<fieldname>' exists, then use its value as the stored
            # value instead.
            if field.stored:
                storedname = "&" + fieldname
                if storedname in fields:
                    stored_value = fields[storedname]
                else :
                    stored_value = value
                doc_field['value'] = stored_value
        
            doc_fields[fieldname] = doc_field

            # commit the child transaction
            field_txn.commit()

        # store the document data
        document = json_encode({'fields': doc_fields})
        logger.trace("doc=%s: data=%s" % (doc_id,document))
        self._segment.set_doc(doc_txn, doc_id, document)

        # return the document id
        return doc_id
    
    def delete_document(self, doc_id, delete=True):
        raise Exception("IndexWriter.delete_document() not implemented")
    
    def delete_by_term(self, fieldname, text, searcher=None):
        raise Exception("IndexWriter.delete_by_term() not implemented")

    def delete_by_query(self, query, searcher=None):
        raise Exception("IndexWriter.delete_by_query() not implemented")

    def _close(self):
        self._ix = None
        self._segment = None
        self._txn = None

    def commit(self):
        # commit the event data
        self._txn.commit()
        # save the last id
        self._ix._lastid = self._lastid
        # close the writer so it can't be used again
        self._close()
        
    def cancel(self):
        # back out the event data
        self._txn.abort()
        # close the writer so it can't be used again
        self._close()
