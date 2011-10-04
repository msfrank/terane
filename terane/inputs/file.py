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

class FileInput(Input):

    def __init__(self):
        self._deferred = None
        self._file = None
        self._prevstats = None
        self._position = None
        self._skipcount = 0
        self._errno = None
        Input.__init__(self)

    def configure(self, section):
        self._path = section.getString('file path', None)
        if self._path == None:
            raise Exception("[input:%s] missing required parameter 'file path'" % self.name)
        logger.debug("[input:%s] path is %s" % (self.name,self._path))
        self._interval = section.getInt('polling interval', 5)
        logger.debug("[input:%s] polling interval is %i seconds" % (self.name,self._interval))
        self._linemax = section.getInt('maximum line length', 1024 * 1024)
        logger.debug("[input:%s] maximum line length is %i bytes" % (self.name,self._linemax))
        self._loopchunk = section.getInt('loop chunk length', 1024 * 1024)
        # loop chunk length has to be at least as big as maximum
        # line length, otherwise we could lose data.
        if self._loopchunk < self._linemax:
            self._loopchunk = self._linemax
        logger.debug("[input:%s] loop chunk length is %i bytes" % (self.name,self._loopchunk))

    def outfields(self):
        return set(('_raw',))

    def startService(self):
        Input.startService(self)
        self._schedule(None)
        logger.debug("[input:%s] started input" % self.name)

    def _schedule(self, loopimmediately):
        self._deferred = Deferred()
        self._deferred.addCallback(self._tail)
        self._deferred.addCallback(self._schedule)
        self._deferred.addErrback(self._error)
        if loopimmediately == True:
            reactor.callLater(0, self._deferred.callback, None)
        else:
            reactor.callLater(self._interval, self._deferred.callback, None)

    def _tail(self, unused):
        try:
            # open the file if necessary
            if self._file == None:
                self._file = open(self._path, 'r')
                _currstats = os.stat(self._path)
                # if this is the first file open, then start reading from
                # the end of the file
                if self._prevstats == None:
                    self._position = _currstats.st_size
                # otherwise start reading from the beginning of the file
                else:
                    self._position = 0
                self._skipcount = 0
                self._prevstats = _currstats
            # get the current file stats
            else:
                _currstats = os.stat(self._path)
        except (IOError,OSError), (errno, errstr):
            if errno != self._errno:
                logger.warning("[input:%s] failed to tail file: %s" % (self.name,errstr))
                self._errno = errno
            return False

        # check if the file inode and/or underlying block device changed
        f = self._file
        try:
            if self._prevstats.st_dev != _currstats.st_dev:
                raise Exception("[input:%s] vfs device changed" % self.name)
            if self._prevstats.st_ino != _currstats.st_ino:
                raise Exception("[input:%s] vfs inode changed" % self.name)
        except Exception, e:
            logger.info(str(e))
            _currstats = os.fstat(self._file.fileno())
            self._file = None
            
        # check if the file has shrunk
        if self._position > _currstats.st_size:
            logger.info("[input:%s] file shrank by %i bytes" %
                (self.name, self._position - _currstats.st_size))
            # reset position to the new end of the file
            self._position = _currstats.st_size
            return False

        # calculate the total bytes available to read
        toread = _currstats.st_size - self._position
        # calculate the bytes we will read this loop iteration
        if toread > self._loopchunk:
            toread = self._loopchunk
            loopimmediately = True
        else:
            loopimmediately = False
        # seek to the next byte to read
        f.seek(self._position)
        # loop reading lines until EOF or incomplete line
        while True:
        
            # save the current position as the start of a new line
            self._position = f.tell()
            # if we have no more bytes to read, then break from the loop
            if toread == 0:
                break

            # calculate the amount of bytes to read for this line
            if toread > self._linemax:
                bufsize = self._linemax
            else:
                bufsize = toread
            line = f.readline(bufsize)
            toread -= len(line)

            # check if the line is incomplete (not newline-terminated).
            # this could occur for three reasons:
            #
            # 1. we have reached the end of the file, and the last line did
            #    not end with a newline.  if we switched to reading from a new
            #    file (e.g. the file was rotated), and we are not currently in
            #    ignore mode (_skipcount is greater than 0) due to a long line,
            #    then we consider this data a full event and write it.  if we
            #    are still in ignore mode, then the data is dropped.  when we
            #    open the new file on the next iteration of _tail(), _skipcount
            #    is reset to 0.
            # 2. the line exceeds the maximum acceptable length for an event.
            #    we enable ignore mode (if its not already enabled) by adding
            #    the length of the line to _skipcount.  data will thus be dropped
            #    until we reach the next newline.
            # 3. we have reached the end of the file, and the last line did
            #    not end with a newline.  if we are still reading from this
            #    file (self._file is not None), then we return to the top of the
            #    loop without writing data.
            if not line.endswith('\n'):
                # case 1: we switched to reading from a new file
                if self._file == None and self._skipcount == 0:
                    self._write(line)
                # case 2: line exceeds _linemax, drop data until the next newline
                elif len(line) == self._linemax:
                    self._skipcount += len(line)
                # case 3: event has not been completely written to disk yet
                else:
                    continue
            # if the line is newline-terminated
            else:
                # if we weren't ignoring the current line, then write it
                if self._skipcount == 0:
                    self._write(line)
                else:
                    self._skipcount += len(line)
                    logger.debug("[input:%s] dropped long line (%i bytes)" %
                        (self.name,self._skipcount))
                    # we found the start of the new event, so stop ignoring data
                    self._skipcount = 0

        # save the old file stats
        self._prevstats = _currstats
        return loopimmediately

    def _write(self, line):
        # ignore lines consisting entirely of whitespace
        line = line.strip()
        if line.isspace():
            return
        logger.trace("[input:%s] received line: %s" % (self.name,line))
        # generate default timestamp and hostname, in case later
        # filters aren't able to add it
        ts = datetime.datetime.now(tzutc())
        hostname = socket.getfqdn()
        fields = {'input':self.name, '_raw':line, 'ts':ts, 'hostname':hostname}
        self.on_received_event.signal(fields)

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

class FileInputPlugin(Plugin):
    factory = FileInput
