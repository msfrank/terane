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

    def __init__(self, ix, txn):
        self._ix = ix
        self._fields = ix._fields
        self._fieldstore = ix._fieldstore
        self._txn = txn

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
            pickled = unicode(pickle.dumps(fieldspec))
            self._ix.set_field(self._txn, fieldname, pickled, NOOVERWRITE=True)
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

    def listFields(self):
        fields = []
        for fieldname,fieldspec in self._fields.items():
            fields += fieldspec.values()
        return succeed(fields)
