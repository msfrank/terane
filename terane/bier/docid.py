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

import time, datetime, struct

class DocID(object):

    MIN_ID = "00000000000000000000000000000000"
    MAX_ID = "ffffffffffffffffffffffffffffffff"

    def __init__(self, ts, node, offset):
        self.ts = int(ts)
        self.node = int(node)
        self.offset = long(offset)

    @classmethod
    def fromPackedBytes(cls, packedbytes):
        ts,node,offset = struct.unpack('>IIQ', packedbytes)
        return DocID(ts, node, offset)

    @classmethod
    def fromString(cls, string):
        ts,node,offset = string[0:8], string[8:16], string[16:32]
        ts = int(ts, 16)
        node = int(node, 16)
        offset = long(offset, 16)
        return DocID(ts, node, offset)

    def pack(self):
        return struct.pack('>IIQ', self.ts, self.node, self.offset)

    def __str__(self):
        return "%.08x%.08x%.016x" % (self.ts, self.node, self.offset)

    def __repr__(self):
        return "<DocID %s>" % str(self)

    def __cmp__(self, other):
        return cmp(str(self), str(other))

    def __add__(self, other):
        other = int(other)
        n = long("%.08x%.08x%.016x" % (self.ts,self.node,self.offset), 16)
        n += other
        if n > 2**128 - 1:
            raise OverflowError()
        return DocID.fromString("%x" % n)

    def __sub__(self, other):
        other = int(other)
        n = long("%.08x%.08x%.016x" % (self.ts,self.node,self.offset), 16)
        n -= other
        if n < 0:
            raise OverflowError()
        return DocID.fromString("%x" % n)
