from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks
import os, sys, time, datetime
from dateutil.tz import tzutc
from zope.interface import implements
from terane.outputs.store import StoreOutput, StoreOutputPlugin
from terane.bier.interfaces import IFieldStore
from terane.bier.event import Event, Contract
from terane.bier.evid import EVID
from terane.bier.fields import IdentityField, TextField
from terane.bier.matching import Every
from terane.bier.searching import searchIndices, Period
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

class Bier_Searching_Tests(unittest.TestCase):
    """bier.searching tests."""

    test_data = [
        (datetime.datetime(2012,1,1,12,0,1,0, tzutc()), 1, u"test1"),
        (datetime.datetime(2012,1,1,12,0,2,0, tzutc()), 2, u"test2"),
        (datetime.datetime(2012,1,1,12,0,3,0, tzutc()), 3, u"test3"),
        (datetime.datetime(2012,1,1,12,0,4,0, tzutc()), 4, u"test4"),
        (datetime.datetime(2012,1,1,12,0,5,0, tzutc()), 5, u"test5"),
        (datetime.datetime(2012,1,1,12,0,6,0, tzutc()), 6, u"test6"),
        (datetime.datetime(2012,1,1,12,0,7,0, tzutc()), 7, u"test7"),
        (datetime.datetime(2012,1,1,12,0,8,0, tzutc()), 8, u"test8"),
        (datetime.datetime(2012,1,1,12,0,9,0, tzutc()), 9, u"test9"),
        (datetime.datetime(2012,1,1,12,0,10,0, tzutc()), 10, u"test10"),
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
        contract = Contract().sign()
        # write events to the index
        for ts,offset,message in Bier_Searching_Tests.test_data:
            event = Event(ts, offset)
            event[contract.field_message] = message
            self.output.receiveEvent(event)

    @inlineCallbacks
    def test_search_Every(self):
        start = EVID.fromDatetime(*Bier_Searching_Tests.test_data[0][0:2])
        end = EVID.fromDatetime(*Bier_Searching_Tests.test_data[-1][0:2])
        period = Period(start, end, False, False)
        resultset = yield searchIndices([self.output.getIndex(),], Every(), period).whenDone()
        results = [EVID(*ev[0]) for ev in resultset.events]
        test_data = [EVID.fromDatetime(ts,offset) for ts,offset,_ in Bier_Searching_Tests.test_data]
        self.assertEqual(results, test_data, "%s != %s" % (
            [str(evid) for evid in results], [str(evid) for evid in test_data]))

    @inlineCallbacks
    def test_search_Every_reverse(self):
        start = EVID.fromDatetime(*Bier_Searching_Tests.test_data[0][0:2])
        end = EVID.fromDatetime(*Bier_Searching_Tests.test_data[-1][0:2])
        period = Period(start, end, False, False)
        resultset = yield searchIndices([self.output.getIndex(),], Every(), period, reverse=True).whenDone()
        results = [EVID(*ev[0]) for ev in resultset.events]
        test_data = sorted([EVID.fromDatetime(ts,offset) for ts,offset,_ in Bier_Searching_Tests.test_data], reverse=True)
        self.assertEqual(results, test_data, "%s != %s" % (
            [str(evid) for evid in results], [str(evid) for evid in test_data]))

    def tearDown(self):
        self.output.stopService()
        self.plugin.stopService()
