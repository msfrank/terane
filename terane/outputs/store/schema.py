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
from terane.registry import getRegistry
from terane import IManager
from terane.bier import IField, ISchema
from terane.bier.fields import QualifiedField
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.schema')

class Schema(object):

    implements(ISchema)

    def __init__(self, index):
        self._index = index
        self._fields = {}
        self._cached = {}
        self._bier = getRegistry().getComponent(IManager, 'bier')
        # load schema data from the db
        with self._index.new_txn() as txn:
            for fieldname,fieldspec in self._index.list_fields(txn):
                self._fields[fieldname] = pickle.loads(fieldspec)
                # verify that the field type is consistent
                for fieldtype,instance in self._fields[fieldname].items():
                    registered = self._bier.getField(fieldtype)
                    if not instance.__class__ == registered.__class__:
                        raise Exception("schema field %s:%s does not match registered type %s" % (
                            fieldname, fieldtype, registered.__class__.__name__))
        # create the field cache
        for fieldname,fieldspec in self._fields.items():
            for fieldtype,instance in fieldspec.items():
                field = QualifiedField(fieldname, fieldtype, instance)
                self._cached[(fieldname,fieldtype)] = field

    def addField(self, fieldname, fieldtype):
        if (fieldname,fieldtype) in self._cached:
            raise KeyError("field %s:%s already exists in Schema" % (fieldname,fieldtype))
        if fieldname in self._fields:
            fieldspec = self._fields[fieldname]
        else:
            fieldspec = {}
        instance = self._bier.getField(fieldtype)
        fieldspec[fieldtype] = instance
        with self._index.new_txn() as txn:
            self._index.add_field(txn, fieldname, pickle.dumps(fieldspec))
        self._fields[fieldname] = fieldspec
        field = QualifiedField(fieldname, fieldtype, instance)
        self._cached[(fieldname,fieldtype)] = field
        return field

    def getField(self, fieldname, fieldtype):
        return self._cached[(fieldname,fieldtype)]

    def hasField(self, fieldname, fieldtype):
        if (fieldname,fieldtype) in self._cached:
            return True
        return False

    def listFields(self):
        return self._cached.itervalues()
