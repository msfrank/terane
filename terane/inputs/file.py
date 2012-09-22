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

import os, sys
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from zope.interface import implements
from terane.plugins import Plugin, IPlugin
from terane.inputs import Input, IInput, Dispatcher
from terane.bier.event import Contract, Assertion
from terane.bier.fields import IdentityField
from terane.loggers import getLogger

logger = getLogger('terane.inputs.file')

class FileInput(Input):

    implements(IInput)

    def __init__(self):
        self._dispatcher = Dispatcher()
        self._delayed = None
        self._deferred = None
        self._file = None
        self._prevstats = None
        self._position = None
        self._skipcount = 0
        self._errno = None
        self._contract = Contract()
        self._contract.addAssertion('_raw', IdentityField, expects=False, guarantees=True, ephemeral=True)
        self._contract.sign() 
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

    def getContract(self):
        return self._contract

    def getDispatcher(self):
        return self._dispatcher

    def startService(self):
        Input.startService(self)
        self._check()
        self._schedule(False)
        logger.debug("[input:%s] started input" % self.name)

    def _schedule(self, value):
        """
        Schedule the next check of self._path for new data.  If value is True,
        then schedule the next loop immediately, otherwise, pause for one loop
        interval.

        :param value: A boolean indicating whether or not to loop immediately.
        :type value: bool
        """
        if self._delayed and self._delayed.active():
            raise Exception("[input:%s] attempted to reschedule twice" % self.name)
        self._deferred = Deferred()
        self._deferred.addCallback(self._tail)
        self._deferred.addCallback(self._schedule)
        if value == True:
            self._delayed = reactor.callLater(0, self._deferred.callback, None)
        else:
            self._delayed = reactor.callLater(self._interval, self._deferred.callback, None)
        logger.trace("[input:%s] rescheduled tail" % self.name)

    def _check(self):
        """
        Return file statistics about self._path.  If there was an error, None is
        returned and self._errno is set.

        :returns: A tuple with the file statistics, or None if there was an error.
        :rtype: tuple or None
        """
        try:
            logger.trace("[input:%s] checking for file modification" % self.name)
            # open the file if necessary
            if self._file == None:
                self._file = open(self._path, 'r')
                currstats = os.stat(self._path)
                # if this is the first file open, then start reading from
                # the end of the file
                if self._prevstats == None:
                    self._position = currstats.st_size
                # otherwise start reading from the beginning of the file
                else:
                    self._position = 0
                self._skipcount = 0
                self._prevstats = currstats
            # get the current file stats
            else:
                currstats = os.stat(self._path)
            return currstats
        except (IOError,OSError), (errno, errstr):
            if errno != self._errno:
                logger.warning("[input:%s] failed to tail file: %s" % (self.name,errstr))
                self._errno = errno
            return None

    def _tail(self, unused):
        """

        :param unused: This parameter is not used for anything, but Twisted
          requires a parameter to be passed to a callback.
        :type: object
        :returns: True if _tail() should be called again immediately, otherwise
          False to indicate we should pause for one loop interval.
        :rtype: bool
        """
        # get the current file statistics
        _currstats = self._check()
        if _currstats == None:
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
        event = self._dispatcher.newEvent()
        event[self._contract.field_input] = self.name
        event[self._contract.field_message] = line
        event[self._contract.field__raw] = line
        self._dispatcher.signalEvent(event)

    def stopService(self):
        if not self.running:
            return
        if self._delayed and self._delayed.active():
            self._delayed.cancel()
        if self._file:
            self._file.close()
        self._file = None
        self._prevstats = None
        self._position = None
        self._deferred = None
        Input.stopService(self)
        logger.debug("[input:%s] stopped input" % self.name)

class FileInputPlugin(Plugin):
    implements(IPlugin)
    factory = FileInput
