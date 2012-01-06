import time, datetime, struct

class DocID(object):
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
        ts,node,offset = string.split('-', 2)
        ts = int(ts, 16)
        node = int(node, 16)
        offset = long(offset, 16)
        return DocID(ts, node, offset)

    def pack(self):
        return struct.pack('>IIQ', self.ts, self.node, self.offset)

    def __str__(self):
        return "%.08x-%.08x-%.016x" % (self.ts, self.node, self.offset)

    def __repr__(self):
        return "<DocID %s>" % str(self)

    def __cmp__(self, other):
        return cmp(str(self), str(other))
