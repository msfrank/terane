# -*- coding: utf-8 -*-

from twisted.trial import unittest
from terane.outputs.store.backend import msgpack_dump, msgpack_load

class Msgpack_Tests(unittest.TestCase):
    """msgpack serialization/deserialization tests."""

    def test_None(self):
        v = None
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_True(self):
        v = True
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_False(self):
        v = False
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_fixnum(self):
        # positive fixnum
        for v in range(-1, 127):
            s = msgpack_dump(v)
            o = msgpack_load(s)
            self.failUnless(o == v, "o=%s, v=%s" % (o,v))
        # negative fixnum
        for v in range(-32, 0):
            s = msgpack_dump(v)
            o = msgpack_load(s)
            self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_uint8(self):
        v = 100
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_int8(self):
        v = -100
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_uint16(self):
        v = 1000
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_int16(self):
        v = -1000
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_uint32(self):
        v = 100000
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_int32(self):
        v = -100000
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_uint64(self):
        v = 10000000
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_int64(self):
        v = -10000000
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_float(self):
        v = float(3.14159)
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_str_fails(self):
        v = str("hello, world!")
        self.failUnlessRaises(ValueError, msgpack_dump, v)

    def test_fixraw(self):
        v = unicode("hello, world!")
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_raw16(self):
        v = unicode("helloworld") * 10
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_raw32(self):
        v = unicode("helloworld") * 10000
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_fixarray(self):
        v = [1,2,3,4,5]
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_array16(self):
        v = [x for x in range(0, 1000)]
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_array32(self):
        v = [x for x in range(0, 100000)]
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_tuple_fails(self):
        v = (6,7,8,9,10)
        self.failUnlessRaises(ValueError, msgpack_dump, v)

    def test_fixmap(self):
        v = {u'foo': u'bar', u'baz': u'qux'}
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))

    def test_map16(self):
        v = dict()
        for x in range(0, 16):
            v[u"k%i" % x] = u"v%i" % x
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "\no=%s\nv=%s" % (o,v))

    def test_map32(self):
        v = dict()
        for x in range(0, 100000):
            v[u"k%i" % x] = u"v%i" % x
        s = msgpack_dump(v)
        o = msgpack_load(s)
        #self.failUnless(o == v, "o=%s, v=%s" % (o,v))
        self.failUnless(o == v)

    def test_dict_key_types(self):
        v = {False: None, 1: None, u'two': None, 3.14159: None, u'foo': None}
        s = msgpack_dump(v)
        o = msgpack_load(s)
        self.failUnless(o == v, "o=%s, v=%s" % (o,v))
