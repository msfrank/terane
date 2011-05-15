# Copyright 2010,2011 Michael Frank <msfrank@syntaxjockey.com>
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

import os, sys, time, datetime, socket
from dateutil.tz import tzutc
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from terane.plugins import Plugin
from terane.inputs import Input
from terane.loggers import getLogger

logger = getLogger('terane.inputs.file')

class FileInputPlugin(Plugin):

    def configure(self, section):
        pass

    def instance(self):
        return FileInput()

class FileInput(Input):

    def __init__(self):
        self._deferred = None
        self._file = None
        self._prevstats = None
        self._position = None
        self._errno = None
        Input.__init__(self)

    def configure(self, section):
        self._path = section.getString('file path', None)
        if self._path == None:
            raise Exception("[input:%s] missing required parameter 'file path'" % self.name)
        logger.debug("[input:%s] path is %s" % (self.name,self._path))
        self._interval = section.getInt('polling interval', 5)
        logger.debug("[input:%s] polling interval is %i seconds" % (self.name,self._interval))
        
    def outfields(self):
        return set(('_raw',))

    def startService(self):
        Input.startService(self)
        self._schedule(None)
        logger.debug("[input:%s] started input" % self.name)

    def _schedule(self, unused):
        self._deferred = Deferred()
        self._deferred.addCallback(self._tail)
        self._deferred.addCallback(self._schedule)
        self._deferred.addErrback(self._error)
        reactor.callLater(self._interval, self._deferred.callback, None)

    def _tail(self, unused):
        try:
            # open the file if necessary
            if self._file == None:
                self._file = open(self._path, 'r')
                self._prevstats = os.stat(self._path)
                self._position = self._prevstats.st_size
                _currstats = self._prevstats
            # get the current file stats
            else:
                _currstats = os.stat(self._path)
        except (IOError,OSError), (errno, errstr):
            if errno != self._errno:
                logger.warning("[input:%s] failed to tail file: %s" % (self.name,errstr))
                self._errno = errno
            return

        # check if the file inode and/or underlying block device changed
        f = self._file
        try:
            if self._prevstats.st_dev != _currstats.st_dev:
                raise Exception("[input:%s] vfs device changed" % self._path)
            if self._prevstats.st_ino != _currstats.st_ino:
                raise Exception("[input:%s] vfs inode changed" % self._path)
        except Exception, e:
            logger.info(str(e))
            _currstats = os.fstat(self._file.fileno())
            self._file = None
            
        # check if the file has shrunk
        if self._position > _currstats.st_size:
            logger.info("[input:%s] file shrank by %i bytes" %
                (self._path, self._position - _currstats.st_size))
            self._position = _currstats.st_size

        # seek to the next byte to read
        f.seek(self._position)
        # loop reading lines until EOF or incomplete line
        while True:
            self._position = f.tell()
            line = f.readline()
            # if we reached EOF or the line consists entirely of whitespace
            if line == '' or line.isspace():
                break
            # if the line is incomplete (not newline-terminated) and
            # we haven't switched to reading from a new file
            if not self._file == None and not line.endswith('\n'):
                break
            # remove leading and trailing whitespace 
            line = line.strip()
            logger.debug("[input:%s] received line: %s" % (self.name,line))
            # generate default timestamp and hostname, in case later
            # filters aren't able to add it
            ts = datetime.datetime.now(tzutc())
            hostname = socket.getfqdn()
            fields = {'input':self.name, '_raw':line, 'ts':ts, 'hostname':hostname}
            self.on_received_event.signal(fields)

        # save the old file stats
        self._prevstats = _currstats

    def _error(self, failure):
        logger.debug("[input:%s] tail error: %s" % (self.name,str(failure)))
        return failure

    def stopService(self):
        if self._file:
            self._file.close()
        self._file = None
        self._prevstats = None
        self._position = None
        self._deferred = None
        Input.stopService(self)
        logger.debug("[input:%s] stopped input" % self.name)
