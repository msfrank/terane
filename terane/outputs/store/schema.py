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
        with self._index.new_txn() as txn:
            for fieldname,fieldspec in self._index.list_fields(txn):
                self._fields[fieldname] = pickle.loads(fieldspec)

    def addField(self, name, field):
        if name in self._fields:
            raise IndexError("field named '%s' already exists in Schema" % name)
        with self._index.new_txn() as txn:
            self._index.add_field(txn, name, pickle.dumps(field))
        self._fields[name] = field

    def getField(self, name):
        return self._fields[name]

    def hasField(self, name):
        if name in self._fields:
            return True
        return False

    def listFields(self):
        return self._fields.keys()
