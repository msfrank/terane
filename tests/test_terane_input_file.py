import os, sys
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.trial import unittest
from terane.loggers import StdoutHandler, startLogging, TRACE
from terane.settings import _UnittestSettings
from terane.inputs.file import FileInput, FileInputPlugin

class FileInput_Tests(unittest.TestCase):
    """FileInput tests."""

    timeout = 10

    def setUp(self):
        self.file_input = None
        self.filename = os.path.abspath(self.mktemp())
        self.f = open(self.filename, 'w')

    def tearDown(self):
        d = Deferred()
        d.addCallback(self._delayedTearDown)
        reactor.callLater(0.1, d.callback, None)
        return d

    def _delayedTearDown(self, unused):
        if self.file_input:
            self.file_input.stopService()
        self.file_input = None
        if self.f:
            self.f.close()
        self.f = None

    def writeLine(self, line):
        self.f.write(line + '\n')
        self.f.flush()
        os.fsync(self.f.fileno())

    def receiveEvent(self, event):
        pass
 
    def test_receive_event(self):
        self.file_input = FileInput()
        self.file_input.setName('test_receive_event')
        settings = _UnittestSettings()
        settings.load({
            'input:tempfile': {
                'file path': self.filename,
                'polling interval': 1
                }
            })
        section = settings.section('input:tempfile')        
        self.file_input.configure(section)
        self.file_input.startService()
        dispatcher = self.file_input.getDispatcher()
        d = dispatcher.connect().addCallback(self.receiveEvent)
        reactor.callLater(1, self.writeLine, "hello world!")
        return d
