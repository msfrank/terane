from twisted.trial import unittest
import os, sys, time, datetime
from dateutil.tz import tzutc
from zope.interface import implements
from terane.outputs.store import StoreOutput, StoreOutputPlugin
from terane.bier.interfaces import IFieldStore
from terane.bier.event import Event, Contract
from terane.bier.fields import IdentityField, TextField
from terane.bier.evid import EVID
from terane.loggers import StdoutHandler, startLogging, TRACE
from terane.settings import _UnittestSettings

class MockFieldStore(object):
    implements(IFieldStore)
    def getField(self, name):
        if name == 'literal':
            return IdentityField(None)
        if name == 'text':
            return TextField(None)
        raise NotImplementedError()

class Output_Store_Tests(unittest.TestCase):
    """outputs.store tests."""

    test_data = [
        (datetime.datetime(2012,1,1,12,0,1,0, tzutc()), 1, u"test1"),
        (datetime.datetime(2012,1,1,12,0,2,0, tzutc()), 2, u"test2"),
        (datetime.datetime(2012,1,1,12,0,3,0, tzutc()), 3, u"test3"),
        (datetime.datetime(2012,1,1,12,0,4,0, tzutc()), 4, u"test4"),
        (datetime.datetime(2012,1,1,12,0,5,0, tzutc()), 5, u"test5"),
        ]

    def setUp(self):
        datadir = os.path.abspath(self.mktemp())
        os.mkdir(datadir)
        settings = _UnittestSettings()
        settings.load({
            'plugin:output:store': {
                'data directory': datadir,
                },
            'output:test': {
                'type': 'store',
                }
            })
        self.plugin = StoreOutputPlugin()
        self.plugin.configure(settings.section('plugin:output:store'))
        self.output = StoreOutput(self.plugin, 'test', MockFieldStore())
        self.output.configure(settings.section('output:test'))
        self.plugin.startService()
        self.output.startService()

    def test_get_contract(self):
        contract = self.output.getContract()
        prior = Contract().sign()
        contract.validateContract(prior)

    def test_read_write_schema(self):
        index = self.output.getIndex()
        schema = index.getSchema()
        schema.addField(u'test_literal', u'literal')
        self.assertTrue(schema.hasField(u'test_literal', u'literal'))
        field = schema.getField(u'test_literal', u'literal')
        self.assertTrue(isinstance(field.field, IdentityField))

    def test_multiple_field_types(self):
        index = self.output.getIndex()
        schema = index.getSchema()
        schema.addField(u'multi_field', u'literal')
        self.assertTrue(schema.hasField(u'multi_field', u'literal'))
        field = schema.getField(u'multi_field', u'literal')
        self.assertTrue(isinstance(field.field, IdentityField))
        schema.addField(u'multi_field', u'text')
        self.assertTrue(schema.hasField(u'multi_field', u'text'))
        field = schema.getField(u'multi_field', u'text')
        self.assertTrue(isinstance(field.field, TextField))

    def test_dynamic_schema(self):
        index = self.output.getIndex()
        schema = index.getSchema()
        contract = Contract().sign()
        ts,offset,message = Output_Store_Tests.test_data[0]
        event = Event(ts, offset)
        event[contract.field_message] = message
        self.output.receiveEvent(event)
        for fieldname,fieldtype,fieldvalue in event:
            self.assertTrue(schema.hasField(fieldname, fieldtype))

    def test_read_write_index(self):
        contract = Contract().sign()
        for ts,offset,message in Output_Store_Tests.test_data:
            event = Event(ts, offset)
            event[contract.field_message] = message
            self.output.receiveEvent(event)
        # read back from the index
        index = self.output.getIndex()
        startId = EVID.fromDatetime(*Output_Store_Tests.test_data[0][0:2])
        endId = EVID.fromDatetime(*Output_Store_Tests.test_data[-1][0:2])
        searcher = index.newSearcher()
        try:
            npostings = searcher.postingsLength(None, None, startId, endId)
            self.assertTrue(npostings == len(Output_Store_Tests.test_data))
            i = searcher.iterPostings(None, None, startId, endId)
            postings = []
            while True:
                posting = i.nextPosting()
                if posting == (None, None, None):
                    break
                postings.append(posting)
        finally:
            searcher.close()

    def tearDown(self):
        self.output.stopService()
        self.plugin.stopService()
