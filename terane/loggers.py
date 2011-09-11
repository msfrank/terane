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

import os, datetime
from Queue import Queue
from twisted.python.log import startLoggingWithObserver, msg, ILogObserver
from twisted.python.util import untilConcludes

NOTSET  = None
FATAL   = 0
ERROR   = 10
WARNING = 20
INFO    = 30
DEBUG   = 40
TRACE   = 50

def _levelToName(level):
    if level == 0:
        return 'FATAL'
    if level == 10:
        return 'ERROR'
    if level == 20:
        return 'WARNING'
    if level == 30:
        return 'INFO'
    if level == 40:
        return 'DEBUG'
    if level >= 50:
        return 'TRACE'
    return 'UNKNOWN'

_observer = None
_records = Queue()
def _ObserverWrapper(record):
    """
    Upon first import of this module, configure logging to simply buffer
    log messages into the _records queue.  Once the application has configured
    logging using startLogging(), the queue is emptied and we simply forward
    log messages to _observer.
    """
    global _observer
    global _records
    if _observer == None:
        _records.put(record)
    else:
        try:
            ts = datetime.datetime.fromtimestamp(record['time'])
        except KeyError:
            ts = datetime.datetime.now()
        record['time'] = ts.isoformat()
        try:
            level = record['level']
        except KeyError:
            level = DEBUG
            record['level'] = level
        try:
            logger = record['logger']
        except KeyError:
            logger = _twistedlogger
            record['logger'] = logger
        if level > logger._level:
            return
        record['loggername'] = str(logger)
        record['levelname'] = _levelToName(level)
        _observer(record)
startLoggingWithObserver(_ObserverWrapper, 0)

class BaseHandler(object):
    def __init__(self):
        pass
    def __call__(self, record):
        record['message'] = record['message'][0]
        self.handle("%(time)s %(levelname)s %(loggername)s: %(message)s" % record)
    def handle(self, message):
        pass
    def close(self):
        pass

class NullHandler(BaseHandler):
    pass

class StdoutHandler(BaseHandler):
    def handle(self, message):
        print message

class FileHandler(BaseHandler):
    def __init__(self, path):
        self._f = open(path, 'a')
    def handle(self, message):
        untilConcludes(self._f.write, message + '\n')
        untilConcludes(self._f.flush)
    def close(self):
        self._f.close()

class Logger(object):
    def __init__(self, name, level):
        self._name = name
        self._level = level
        self._children = {}

    def __str__(self):
        return self._name

    def msg(self, level, message, **kwds):
        kwds['logger'] = self
        kwds['level'] = level
        msg(message, **kwds)

    def trace(self, message, **kwds):
        self.msg(TRACE, message, **kwds)

    def debug(self, message, **kwds):
        self.msg(DEBUG, message, **kwds)

    def info(self, message, **kwds):
        self.msg(INFO, message, **kwds)

    def warning(self, message, **kwds):
        self.msg(WARNING, message, **kwds)

    def error(self, message, **kwds):
        self.msg(ERROR, message, **kwds)

_loggers = Logger('', NOTSET)
def getLogger(name):
    """
    Return the logger specified by `name`.

    :param name: The name of the logger.
    :type name: str
    :returns: The Logger instance.
    :rtype: :class:`terane.loggers.Logger`
    """
    if name == '':
        raise Exception("cannot return root Logger")
    global _loggers
    logger = _loggers
    parts = []
    for p in name.split('.'):
        if p == '':
            raise Exception("logger '%s' is invalid: domain component cannot be empty" % name)
        parts.append(p)
        try:
            child = logger._children[p]
        except KeyError:
            child = Logger('.'.join(parts), logger._level)
            logger._children[p] = child
        logger = child
    return logger
_twistedlogger = getLogger('twisted')

def startLogging(observer, level=INFO, configfile=None):
    """
    Start logging messages using the supplied `observer`.  All buffered messages are
    flushed.  Log messages which have a level less than `level` are dropped, unless
    the logger has been specifically configured differently in the supplied `configfile`.  

    :param observer: The observer to which will output messages.
    :type handler: callable or implements :class:`twisted.python.log.ILogObserver`
    :param level: The default logging level.
    :type level: int
    :param configfile: A path to the logging config file, or None.
    :type configfile: str
    """
    global _observer
    global _loggers
    if observer == None:
        _observer = NullHandler()
    else:
        _observer = observer
    try:
        level = int(level)
        _loggers._level = level
    except:
        raise Exception("level must be an int")
    def _setLevel(l, v):
        l._level = v
        for l in l._children.values():
            _setLevel(l, v)
    _setLevel(_loggers, level)
    # Load the logging configuration from the specified configuration file.
    if configfile != None and os.access(configfile, os.R_OK):
        with file(configfile, 'r') as f:
            for line in f.readlines():
                line.strip()
                if line == '' or line[0] == '#':
                    continue
                try:
                    name,level = [v.strip() for v in line.split('=', 1)]
                except:
                    continue
                logger = getLogger(name)
                level.upper()
                if level == 'TRACE':
                    _setLevel(logger, TRACE)
                elif level == 'DEBUG':
                    _setLevel(logger, DEBUG)
                elif level == 'INFO':
                    _setLevel(logger, INFO)
                elif level == 'WARNING':
                    _setLevel(logger, WARNING)
                elif level == 'ERROR':
                    _setLevel(logger, ERROR)
    # flush all buffered log messages to the real observer
    while not _records.empty():
        _ObserverWrapper(_records.get())

def stopLogging():
    pass

def restartLogging(observer, level, configfile=None):
    pass
