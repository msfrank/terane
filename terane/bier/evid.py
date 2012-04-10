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

import time, datetime, dateutil.tz, struct, calendar

class EVID(object):

    MIN_ID = "00000000000000000000000000000000"
    MAX_ID = "ffffffffffffffffffffffffffffffff"

    def __init__(self, ts, uuid):
        self.ts = int(ts)
        self.node = int(node)
        self.offset = long(offset)

    @classmethod
    def fromPackedBytes(cls, packedbytes):
        ts,node,offset = struct.unpack('>IQ', packedbytes)
        return EVID(ts, node, offset)

    @classmethod
    def fromString(cls, string):
        ts,node,offset = string[0:8], string[8:16], string[16:32]
        ts = int(ts, 16)
        node = int(node, 16)
        offset = long(offset, 16)
        return EVID(ts, node, offset)

    @classmethod
    def fromDatetime(cls, dt=None):
        if dt == None:
            dt = datetime.datetime.now()
        elif not isinstance(dt, datetime.datetime):
            raise TypeError("dt must be a datetime.datetime or None")
        # if no timezone is specified, then assume local tz
        if dt.tzinfo == None:
            dt = dt.replace(tzinfo=dateutil.tz.tzlocal())
        # convert to UTC, if necessary
        if not dt.tzinfo == dateutil.tz.tzutc():
            dt = dt.astimezone(dateutil.tz.tzutc())
        return EVID(int(calendar.timegm(dt.timetuple())), 0, 0)

    def pack(self):
        return struct.pack('>IIQ', self.ts, self.node, self.offset)

    def __str__(self):
        return "%.08x%.08x%.016x" % (self.ts, self.node, self.offset)

    def __repr__(self):
        return "<EVID %s>" % str(self)

    def __cmp__(self, other):
        return cmp(str(self), str(other))

    def __add__(self, other):
        other = int(other)
        n = long("%.08x%.08x%.016x" % (self.ts,self.node,self.offset), 16)
        n += other
        if n > 2**128 - 1:
            raise OverflowError()
        return EVID.fromString("%x" % n)

    def __sub__(self, other):
        other = int(other)
        n = long("%.08x%.08x%.016x" % (self.ts,self.node,self.offset), 16)
        n -= other
        if n < 0:
            raise OverflowError()
        return EVID.fromString("%x" % n)
