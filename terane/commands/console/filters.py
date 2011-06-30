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

import re

class Is(object):
    def __init__(self, field, params):
        self._field = field
        self._value = params[0]

    def __call__(self, fields):
        if self._field == None:
            for k,v in fields.items():
                if v == self._value:
                    return True
        else:
            try:
                if fields[self._field] == self._value:
                    return True
            except:
                pass
        return False

class Matches(object):
    def __init__(self, field, params):
        self._field = field
        self._regex = re.compile(params[0])

    def __call__(self, fields):
        if self._field == None:
            for k,v in fields.items():
                if self._regex.search(v) != None:
                    return True
        else:
            try:
                if self._regex.search(fields[self._field]) != None:
                    return True
            except:
                pass
        return False

class Contains(object):
    def __init__(self, field, params):
        self._field = field
        self._sub = params[0]

    def __call__(self, fields):
        if self._field == None:
            for k,v in fields.items():
                if v.find(self._sub) > -1:
                    return True
        else:
            try:
                if fields[self._field].find(self._sub) > -1:
                    return True
            except:
                pass
        return False
                
class GreaterThan(object):
    def __init__(self, field, params):
        self._field = field
        value = params[0]
        if value.find('.') > -1:
            self._type = float
        else:
            self._type = int
        self._value = self._type(value)

    def __call__(self, fields):
        if self._field == None:
            for k,v in fields.items():
                try:
                    if self._type(v) > self._value:
                        return True
                except:
                    pass
        else:
            try:
                if self._type(fields[self._field]) > self._value:
                    return True
            except:
                pass
        return False

class LessThan(object):
    def __init__(self, field, params):
        self._field = field
        value = params[0]
        if value.find('.') > -1:
            self._type = float
        else:
            self._type = int
        self._value = self._type(value)

    def __call__(self, fields):
        if self._field == None:
            for k,v in fields.items():
                try:
                    if self._type(v) < self._value:
                        return True
                except:
                    pass
        else:
            try:
                if self._type(fields[self._field]) < self._value:
                    return True
            except:
                pass
        return False
