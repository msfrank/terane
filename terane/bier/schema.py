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

import datetime, time, struct, base64
from terane.loggers import getLogger

logger = getLogger('terane.bier.schema')

class BaseField(object):
    def __init__(self, options):
        self.options = options

    def terms(self, value):
        raise NotImplemented()

class IdentityField(BaseField):
    def terms(self, value):
        if not isinstance(value, list) and not isinstance(value, tuple):
            raise Exception("value '%s' is not of type list or tuple" % value)
        return iter([(unicode(t.strip()),None) for t in value])

class TextField(BaseField):
    def terms(self, value):
        if not isinstance(value, unicode) and not isinstance(value, str):
            raise Exception("value '%s' is not of type unicode or str" % value)
        return iter([(unicode(t),None) for t in value.split() if len(t) > 0])

class DatetimeField(BaseField):
    def terms(self, value):
        if not isinstance(value, datetime.datetime):
            raise Exception("value '%s' is not of type datetime.datetime" % value)
        # calculate the unix timestamp, with 1 second accuracy
        ts = int(time.mktime(value.timetuple()))
        # pack the int as a 32 bit big-endian integer
        ts = struct.pack(">I", ts)
        # convert the packed int to base64   
        ts = unicode(base64.standard_b64encode(ts))
        return iter([(ts, None)])

def fieldFactory(evalue, **options):
    if isinstance(evalue, str) or isinstance(evalue, unicode):
        return TextField(options)
    if isinstance(evalue, datetime.datetime):
        return DatetimeField(options)
    if isinstance(evalue, list) or isinstance(evalue, tuple):
        return IdentityField(options)
    raise TypeError("unknown event value type '%s'" % str(type(evalue)))
