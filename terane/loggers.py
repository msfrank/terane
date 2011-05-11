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

import os
from Queue import Queue
from logging import Logger, Handler, Formatter, StreamHandler, FileHandler
from logging import getLogger as _getLogger
from logging import NOTSET, DEBUG, INFO, WARNING, ERROR
from twisted.application.service import Service

class NullHandler(Handler):
    """
    A simple handler which discards log messages.
    """
    def emit(self, record):
        pass

_records = Queue()

class BufferingHandler(Handler):
    """
    A handler which buffers log messages in a queue.
    """
    def emit(self, record):
        _records.put(record)

_buffer = BufferingHandler()
_root = _getLogger('')
_root.addHandler(_buffer)
_root.setLevel(NOTSET)

def _resetLoggerHandlers(logger, handler):
    if isinstance(logger, Logger):
        # remove the buffer handler, add the 'real' handler
        if _buffer in logger.handlers:
            logger.addHandler(handler)
            logger.removeHandler(_buffer)
        # if level is NOTSET then reset to self._level
        if logger.level == NOTSET:
            logger.setLevel(handler.level)

def startLogging(handler):
    """
    Start logging messages using the supplied `handler`.  All buffered
    messages are flushed.  If `handler` is None, then use the :class:`NullHandler`.  

    :param handler: The handler to which will display messages.
    :type handler: :class:`logging.Handler`
    """
    if handler == None:
        handler = NullHandler()
    # switch from buffer to the real handler
    _resetLoggerHandlers(_getLogger(''), handler)
    for logger in _root.manager.loggerDict.values():
        _resetLoggerHandlers(logger, handler)
    # flush all buffered log messages to the real handler
    while not _records.empty():
        record = _records.get()
        logger = _getLogger(record.name)
        if logger.isEnabledFor(record.levelno):
            logger.handle(record)

def getLogger(name):
    """
    Return the logger specified by `name`.

    :param name: The name of the logger.
    :type name: str
    :returns: The Logger instance.
    :rtype: :class:`logging.Logger`
    """
    return _getLogger(name)
