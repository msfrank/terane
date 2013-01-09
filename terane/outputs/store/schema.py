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

import pickle
from zope.interface import implements
from twisted.internet.defer import succeed, fail
from terane.bier import IField, ISchema
from terane.bier.fields import QualifiedField
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.schema')

class Schema(object):

    implements(ISchema)

    def __init__(self, index, fieldstore):
        self._index = index
        self._fields = {}
        self._fieldstore = fieldstore
        # load schema data from the db
        with self._index.new_txn() as txn:
            for fieldname,fieldspec in self._index.iter_fields(txn):
                self._fields[fieldname] = pickle.loads(str(fieldspec))
                # verify that the field type is consistent
                for fieldtype,stored in self._fields[fieldname].items():
                    field = fieldstore.getField(fieldtype)
                    if not stored.field.__class__ == field.__class__:
                        raise Exception("schema field %s:%s does not match registered type %s" % (
                            fieldname, fieldtype, field.__class__.__name__))

    def addField(self, fieldname, fieldtype):
        try:
            if fieldname in self._fields:
                fieldspec = self._fields[fieldname]
            else:
                fieldspec = {}
            if fieldtype in fieldspec:
                raise KeyError("field %s:%s already exists in Schema" % (fieldname,fieldtype))
            field = self._fieldstore.getField(fieldtype)
            stored = QualifiedField(fieldname, fieldtype, field)
            fieldspec[fieldtype] = stored
            with self._index.new_txn() as txn:
                self._index.add_field(txn, fieldname, unicode(pickle.dumps(fieldspec)))
            self._fields[fieldname] = fieldspec
            return succeed(stored)
        except Exception, e:
            return fail(e)

    def getField(self, fieldname, fieldtype):
        try:
            if fieldname == None and fieldtype == None:
                return self.getField(u'message', None)
            if fieldname not in self._fields:
                raise KeyError("%s:%s" % (fieldname, fieldtype))
            fieldspec = self._fields[fieldname]
            if fieldtype in fieldspec:
                return succeed(fieldspec[fieldtype])
            if fieldtype == None and len(fieldspec) == 1:
                return succeed(fieldspec.values()[0])
            raise KeyError("%s:%s" % (fieldname, fieldtype))
        except Exception, e:
            return fail(e)

    def hasField(self, fieldname, fieldtype):
        try:
            if fieldname not in self._fields:
                return succeed(False)
            fieldspec = self._fields[fieldname]
            if fieldtype not in fieldspec:
                return succeed(False)
            return True
        except Exception, e:
            return fail(e)

    def listFields(self):
        return succeed(self._cached.values())
