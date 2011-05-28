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

import os, sys, curses, xmlrpclib, dateutil.parser
from twisted.internet import reactor
from twisted.web.xmlrpc import Proxy
from terane.loggers import getLogger

logger = getLogger('terane.commands.drill.driller')

class Searcher(object):
    def __init__(self, screen, host, query):
        self._screen = screen
        screen.setWindow(self)
        self._host = host
        self._query = query
        self._meta = None
        self._results = None
        self._pad = None
        self._width = None
        self._height = None
        self._pos = 0
        # make the search request
        proxy = Proxy("http://%s/XMLRPC" % self._host, allowNone=True)
        deferred = proxy.callRemote('search', self._query)
        deferred.addCallback(self._getResult)
        deferred.addErrback(self._getError)
        
    def _getResult(self, results):
        self._meta = results.pop(0)
        self._results = [Result(r) for r in results]
        self._screen.refresh()

    def _getError(self, failure):
        try:
            raise failure.value
        except xmlrpclib.Fault, e:
            logger.debug("search failed: %s (code %i)" % (e.faultString,e.faultCode))
        except BaseException, e:
            logger.debug("search failed: %s" % str(e))

    def refresh(self, y, x, height, width):
        # calculate the height of the output
        lineh = 0
        for r in self._results:
            r._resize(width)
            lineh += r.height
        # if the screen width has changed, or the line
        # height exceeds the output window height, then
        # redraw the lines to the output window
        if width != self._width or lineh > height:
            self._pad = curses.newpad(lineh, width)
            self._width = width
            self._height = lineh
            logger.debug("allocated new output pad of size %i x %i" % (width, lineh))
            i = 0
            for r in self._results:
                r.draw(self._pad, i, 0)
                i += r.height
        else:
            logger.debug("keeping current output pad of size %i x %i" % (self._width,self._height))
        # refresh the output window
        self._pad.refresh(self._pos, 0, y, x, height - 1, width - 1)
        logger.debug("refresh: spos=%i,%i pos=%i,%i size=%ix%i" % (0,self._pos,x,y,width-1,height-1))

class Result(object):
    def __init__(self, fields):
        default = fields['default']
        ts = dateutil.parser.parse(fields['ts']).strftime("%d %b %Y %H:%M:%S")
        self._line = "%s: %s" % (ts, default)
        self.width = 0
        self.height = 0

    def _resize(self, screenw):
        if len(self._line) == 0:
            self.width, self.height = screenw, 1
        else:
            q,r = divmod(len(self._line), screenw)
            if r > 0: q += 1
            self.width, self.height = screenw, q

    def draw(self, win, y, x):
        win.addstr(y, x, self._line)
