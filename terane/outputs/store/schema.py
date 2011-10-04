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
from collections import MutableMapping
from whoosh.fields import Schema as WhooshSchema, FieldConfigurationError
from terane.db.storage import Txn
from terane.loggers import getLogger

logger = getLogger('terane.db.schema')

class FieldDict(MutableMapping):
    """
    FieldDict is a proxy object to interface with the schema table in the index.
    """
    def __init__(self, env, toc):
        self._env = env
        self._toc = toc

    def __getitem__(self, fieldname):
        fieldname = str(fieldname)
        with Txn(self._env) as txn:
            return pickle.loads(self._toc.get_field(txn, fieldname))

    def __setitem__(self, fieldname, fieldspec):
        fieldname = str(fieldname)
        with Txn(self._env) as txn:
            self._toc.add_field(txn, fieldname, pickle.dumps(fieldspec))

    def __delitem__(self, fieldname):
        fieldname = str(fieldname)
        with Txn(self._env) as txn:
            self._toc.remove_field(txn, fieldname)

    def __contains__(self, fieldname):
        fieldname = str(fieldname)
        with Txn(self._env) as txn:
            return self._toc.contains_field(txn, fieldname)

    def __len__(self):
        return self._toc.count_fields()

    def __iter__(self):
        with Txn(self._env) as txn:
            return iter([k for k,v in self._toc.list_fields(txn)])

    def __eq__(self, other):
        with Txn(self._env) as txn:
            fd1 = sorted(self._toc.list_fields(txn))
            fd2 = sorted(other._toc.list_fields(txn))
            return cmp(fd1, fd2)

class Schema(WhooshSchema):
    """
    A thin wrapper over whoosh.fields.Schema to use the schema from the index.
    """
    def __init__(self, env, toc):
        self._fields = FieldDict(env, toc)
        self._dyn_fields = {}

    def add(self, name, fieldtype, glob=False):
        if glob != False:
            raise FieldConfigurationError("dynamic fields are not supported")
        WhooshSchema.add(self, name, fieldtype, False)
