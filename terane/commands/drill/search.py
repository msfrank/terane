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

import os, sys, urwid
from dateutil.parser import parse
from xmlrpclib import Fault
from twisted.internet import reactor
from twisted.web.xmlrpc import Proxy
from terane.loggers import getLogger

logger = getLogger('terane.commands.drill.driller')

class Searcher(urwid.WidgetWrap):
    def __init__(self, loop, host, query):
        self._loop = loop
        self._host = host
        self._query = query
        self._meta = None
        self._results = None
        self._pos = 0
        # make the search request
        proxy = Proxy("http://%s/XMLRPC" % self._host, allowNone=True)
        deferred = proxy.callRemote('search', self._query)
        deferred.addCallback(self._getResult)
        deferred.addErrback(self._getError)
        # build the listbox widget
        self._walker = urwid.SimpleListWalker([])
        self._listbox = urwid.ListBox(self._walker)
        urwid.WidgetWrap.__init__(self, self._listbox)
        
    def _getResult(self, results):
        reactor.callFromThread(self._displayResult, results)

    def _displayResult(self, results):
        self._meta = results.pop(0)
        for r in results:
            default = r['default']
            ts = parse(r['ts']).strftime("%d %b %Y %H:%M:%S")
            self._walker.append(urwid.Text("%s: %s" % (ts, default)))
        self._loop.draw_screen()

    def _getError(self, failure):
        reactor.callFromThread(self._displayError, failure)

    def _displayError(self, failure):
        try:
            raise failure.value
        except Fault, e:
            logger.debug("search failed: %s (code %i)" % (e.faultString,e.faultCode))
            self._walker.append(urwid.Text("search failed: %s (code %i)" % (e.faultString,e.faultCode)))
        except BaseException, e:
            logger.debug("search failed: %s" % str(e))
            self._walker.append(urwid.Text("search failed: %s" % str(e)))
        self._loop.draw_screen()
