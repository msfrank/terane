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

import pickle
from zope.interface import implements
from terane.bier import ISchema
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.schema')

class Schema(object):

    implements(ISchema)

    def __init__(self, index):
        self._index = index
        self._fields = {}
        self._cached = {}
        with self._index.new_txn() as txn:
            for fieldname,fieldspec in self._index.list_fields(txn):
                self._fields[fieldname] = pickle.loads(fieldspec)
        for fieldname,fieldspec in self._fields.items():
            for fieldtype in fieldspec:
                self._cached[(fieldname,fieldtype)] = fieldtype()

    def addField(self, fieldname, fieldtype):
        if (fieldname,fieldtype) in self._cached:
            raise KeyError("field %s:%s already exists in Schema" % (fieldname,fieldtype.__name__))
        if fieldname in self._fields:
            fieldspec = self._fields[fieldname]
        else:
            fieldspec = []
        fieldspec.append(fieldtype)
        with self._index.new_txn() as txn:
            self._index.add_field(txn, fieldname, pickle.dumps(fieldspec))
        self._fields[fieldname] = fieldspec
        field = fieldtype()
        self._cached[(fieldname,fieldtype)] = field
        return field

    def getField(self, fieldname, fieldtype):
        return self._cached[(fieldname,fieldtype)]

    def hasField(self, fieldname, fieldtype):
        if (fieldname,fieldtype) in self._cached:
            return True
        return False

    def listFields(self):
        return [(f[0],f[1],spec) for f,spec in self._cached.iteritems()]

