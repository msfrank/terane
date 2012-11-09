import os
from twisted.trial import unittest
from terane.outputs.store.backend import Env, Index, Segment

class Backend_Env_Tests(unittest.TestCase):
    """backend.Env tests."""

    def test_create_Env(self):
        root = os.path.abspath(self.mktemp())
        os.mkdir(root)
        datadir = os.path.join(root, 'data')
        os.mkdir(datadir)
        envdir = os.path.join(root, 'env')
        os.mkdir(envdir)
        tmpdir = os.path.join(root, 'tmp')
        os.mkdir(tmpdir)
        options = {}
        env = Env(envdir, datadir, tmpdir, options)
        env.close()

class Backend_Index_Tests(unittest.TestCase):

    def setUp(self):
        root = os.path.abspath(self.mktemp())
        os.mkdir(root)
        datadir = os.path.join(root, 'data')
        os.mkdir(datadir)
        envdir = os.path.join(root, 'env')
        os.mkdir(envdir)
        tmpdir = os.path.join(root, 'tmp')
        os.mkdir(tmpdir)
        options = {}
        self.env = Env(envdir, datadir, tmpdir, options)

    def test_create_Index(self):
        index = Index(self.env, 'store')
        index.close()

    def test_read_write_meta(self):
        index = Index(self.env, 'store')
        try:
            with index.new_txn() as txn:
                index.set_meta(txn, u'foo', True)
            self.failUnless(index.get_meta(None, u'foo') == True)
        finally:
            index.close()

    def test_read_write_field(self):
        index = Index(self.env, 'store')
        try:
            fieldname = u'fieldname'
            fieldspec = {u'fieldtype': u'pickledfield'}
            with index.new_txn() as txn:
                index.add_field(txn, fieldname, fieldspec)
            self.failUnless(index.get_field(None, fieldname) == fieldspec)
            fields = index.list_fields(None)
            self.failUnless(len(fields) == 1)
            self.failUnless(fields[0] == (fieldname,fieldspec))
        finally:
            index.close()

    def tearDown(self):
        self.env.close()
        
class Backend_Segment_Tests(unittest.TestCase):

    def setUp(self):
        root = os.path.abspath(self.mktemp())
        os.mkdir(root)
        datadir = os.path.join(root, 'data')
        os.mkdir(datadir)
        envdir = os.path.join(root, 'env')
        os.mkdir(envdir)
        tmpdir = os.path.join(root, 'tmp')
        os.mkdir(tmpdir)
        options = {}
        self.env = Env(envdir, datadir, tmpdir, options)
        self.index = Index(self.env, 'store')

    def test_create_Segment(self):
        try:
            segment = None
            with self.index.new_txn() as txn:
                segmentId = self.index.new_segment(txn)
                segment = Segment(txn, self.index, segmentId)
            self.failUnless(segmentId == 1)
        finally:
            if segment: segment.close()
        try:
            segment = None
            with self.index.new_txn() as txn:
                segmentId = self.index.new_segment(txn)
                segment = Segment(txn, self.index, segmentId)
            self.failUnless(segmentId == 2)
        finally:
            if segment: segment.close()
        try:
            segment = None
            with self.index.new_txn() as txn:
                segmentId = self.index.new_segment(txn)
                segment = Segment(txn, self.index, segmentId)
            self.failUnless(segmentId == 3)
        finally:
            if segment: segment.close()
        self.failUnless(sorted(self.index.iter_segments(None)) == [1,2,3])

    def test_delete_Segment(self):
        with self.index.new_txn() as txn:
            segmentId = self.index.new_segment(txn)
        with self.index.new_txn() as txn:
            self.index.delete_segment(txn, segmentId)

    def tearDown(self):
        self.index.close()
        self.env.close()
