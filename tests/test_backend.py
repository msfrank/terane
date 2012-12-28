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
            fields = list(index.iter_fields(None))
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
                self.index.add_segment(txn, u'store.1', None)
                segment = Segment(self.env, txn, u'store.1')
        finally:
            if segment: segment.close()
        try:
            segment = None
            with self.index.new_txn() as txn:
                self.index.add_segment(txn, u'store.2', None)
                segment = Segment(self.env, txn, u'store.2')
        finally:
            if segment: segment.close()
        try:
            segment = None
            with self.index.new_txn() as txn:
                self.index.add_segment(txn, u'store.3', None)
                segment = Segment(self.env, txn, u'store.3')
        finally:
            if segment: segment.close()
        segments = [name for name,_ in self.index.iter_segments(None)]
        self.failUnless(segments == [u'store.1', u'store.2', u'store.3'])

    def test_delete_Segment(self):
        with self.index.new_txn() as txn:
            self.index.add_segment(txn, u'store.1', None)
            segment = Segment(self.env, txn, u'store.1')
        with self.index.new_txn() as txn:
            self.index.delete_segment(txn, u'store.1')

    def test_read_write_meta(self):
        try:
            segment = None
            with self.index.new_txn() as txn:
                self.index.add_segment(txn, u'store.1', None)
                segment = Segment(self.env, txn, u'store.1')
                segment.set_meta(txn, u'foo', True)
            self.failUnless(segment.get_meta(None, u'foo') == True)
        finally:
            if segment: segment.close()

    def test_read_write_field(self):
        try:
            key = [u'fieldname', u'fieldtype']
            segment = None
            with self.index.new_txn() as txn:
                self.index.add_segment(txn, u'store.1', None)
                segment = Segment(self.env, txn, u'store.1')
            with self.index.new_txn() as txn:
                segment.set_field(txn, key, True)
            self.failUnless(segment.get_field(None, key) == True)
        finally:
            if segment: segment.close()

    def test_read_write_event(self):
        try:
            segment = None
            with self.index.new_txn() as txn:
                self.index.add_segment(txn, u'store.1', None)
                segment = Segment(self.env, txn, u'store.1')
            key = [1, 1]
            # test writing an event, then retreiving it
            with self.index.new_txn() as txn:
                segment.set_event(txn, key, True)
            self.failUnless(segment.get_event(None, key) == True)
            # test verifying the existence of the event
            self.failUnless(segment.contains_event(None, key) == True)
            # test modifying the same event, then retrieving it
            with self.index.new_txn() as txn:
                segment.set_event(txn, key, False)
            self.failUnless(segment.get_event(None, key) == False)
        finally:
            if segment: segment.close()

    def test_read_write_term(self):
        try:
            segment = None
            with self.index.new_txn() as txn:
                self.index.add_segment(txn, u'store.1', None)
                segment = Segment(self.env, txn, u'store.1')
            key = [u'fieldname', u'fieldtype', u'foo']
            # test writing an event, then retreiving it
            with self.index.new_txn() as txn:
                segment.set_term(txn, key, True)
            self.failUnless(segment.get_term(None, key) == True)
            # test modifying the same event, then retrieving it
            with self.index.new_txn() as txn:
                segment.set_event(txn, key, False)
            self.failUnless(segment.get_event(None, key) == False)
        finally:
            if segment: segment.close()

    def test_read_write_posting(self):
        try:
            key = [u'fieldname', u'fieldtype', u'foo', 1, 1]
            segment = None
            with self.index.new_txn() as txn:
                self.index.add_segment(txn, u'store.1', None)
                segment = Segment(self.env, txn, u'store.1')
            # test writing an event, then retreiving it
            with self.index.new_txn() as txn:
                segment.set_posting(txn, key, True)
            self.failUnless(segment.get_posting(None, key) == True)
            # test verifying the existence of the event
            self.failUnless(segment.contains_posting(None, key) == True)
            # test modifying the same event, then retrieving it
            with self.index.new_txn() as txn:
                segment.set_posting(txn, key, False)
            self.failUnless(segment.get_posting(None, key) == False)
        finally:
            if segment: segment.close()

    def test_iter_postings(self):
        try:
            prefix = [u'fieldname', u'fieldtype', u'foo']
            segment = None
            with self.index.new_txn() as txn:
                self.index.add_segment(txn, u'store.1', None)
                segment = Segment(self.env, txn, u'store.1')
                segment.set_posting(txn, prefix + [1,1], True)
                segment.set_posting(txn, prefix + [1,2], True)
                segment.set_posting(txn, prefix + [1,3], True)
                segment.set_posting(txn, prefix + [1,4], True)
                segment.set_posting(txn, prefix + [1,5], True)
            # test iterating all postings
            with self.index.new_txn() as txn:
                postings = list(segment.iter_postings(txn, None, None, False))
                self.failUnless(postings == [
                    (prefix + [1,1], True),
                    (prefix + [1,2], True),
                    (prefix + [1,3], True),
                    (prefix + [1,4], True),
                    (prefix + [1,5], True),
                    ])
            # test iterating a subset of postings
            with self.index.new_txn() as txn:
                postings = list(segment.iter_postings(txn, prefix + [1,2], prefix + [1,4], False))
                self.failUnless(postings == [
                    (prefix + [1,2], True),
                    (prefix + [1,3], True),
                    (prefix + [1,4], True),
                    ])
            # test iterating from a specified key
            with self.index.new_txn() as txn:
                postings = list(segment.iter_postings(txn, prefix + [1,3], None, False))
                self.failUnless(postings == [
                    (prefix + [1,3], True),
                    (prefix + [1,4], True),
                    (prefix + [1,5], True),
                    ])
            # test iterating until a specified key
            with self.index.new_txn() as txn:
                postings = list(segment.iter_postings(txn, None, prefix + [1,3], False))
                self.failUnless(postings == [
                    (prefix + [1,1], True),
                    (prefix + [1,2], True),
                    (prefix + [1,3], True),
                    ])
        finally:
            if segment: segment.close()

    def test_iter_postings_reverse(self):
        try:
            prefix = [u'fieldname', u'fieldtype', u'foo']
            segment = None
            with self.index.new_txn() as txn:
                self.index.add_segment(txn, u'store.1', None)
                segment = Segment(self.env, txn, u'store.1')
                segment.set_posting(txn, prefix + [1,1], True)
                segment.set_posting(txn, prefix + [1,2], True)
                segment.set_posting(txn, prefix + [1,3], True)
                segment.set_posting(txn, prefix + [1,4], True)
                segment.set_posting(txn, prefix + [1,5], True)
            # test iterating all postings in reverse order
            with self.index.new_txn() as txn:
                postings = list(segment.iter_postings(txn, None, None, True))
                self.failUnless(postings == [
                    (prefix + [1,5], True),
                    (prefix + [1,4], True),
                    (prefix + [1,3], True),
                    (prefix + [1,2], True),
                    (prefix + [1,1], True),
                    ])
            # test iterating a subset of postings in reverse order
            with self.index.new_txn() as txn:
                postings = list(segment.iter_postings(txn, prefix + [1,2], prefix + [1,4], True))
                self.failUnless(postings == [
                    (prefix + [1,4], True),
                    (prefix + [1,3], True),
                    (prefix + [1,2], True),
                    ])
            # test iterating from a specified key in reverse order
            with self.index.new_txn() as txn:
                postings = list(segment.iter_postings(txn, prefix + [1,3], None, True))
                self.failUnless(postings == [
                    (prefix + [1,5], True),
                    (prefix + [1,4], True),
                    (prefix + [1,3], True),
                    ])
            # test iterating until a specified key in reverse order
            with self.index.new_txn() as txn:
                postings = list(segment.iter_postings(txn, None, prefix + [1,3], True))
                self.failUnless(postings == [
                    (prefix + [1,3], True),
                    (prefix + [1,2], True),
                    (prefix + [1,1], True),
                    ])
        finally:
            if segment: segment.close()

    def tearDown(self):
        self.index.close()
        self.env.close()
