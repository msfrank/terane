import os, sys
from twisted.trial import unittest
import terane.db

class db_storage_Env_tests(unittest.TestCase):

    def test_create_Env(self):
        tmproot = os.path.abspath(self.mktemp())
        os.mkdir(tmproot)
        datadir = os.path.join(tmproot, 'data')
        os.mkdir(datadir)
        envdir = os.path.join(tmproot, 'env')
        os.mkdir(envdir)
        tmpdir = os.path.join(tmproot, 'tmp')
        os.mkdir(tmpdir)
        env = terane.db.storage.Env(envdir, datadir, tmpdir)
        env.close()

class db_storage_Store_tests(unittest.TestCase):

    def setUp(self):
        tmproot = os.path.abspath(self.mktemp())
        os.mkdir(tmproot)
        datadir = os.path.join(tmproot, 'data')
        os.mkdir(datadir)
        envdir = os.path.join(tmproot, 'env')
        os.mkdir(envdir)
        tmpdir = os.path.join(tmproot, 'tmp')
        os.mkdir(tmpdir)
        self.env = terane.db.storage.Env(envdir, datadir, tmpdir)

    def test_create_Store(self):
        store = terane.db.storage.Store(self.env, 'store')
        store.close()

    def tearDown(self):
        self.env.close()
