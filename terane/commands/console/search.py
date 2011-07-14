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
from csv import DictWriter
from twisted.web.xmlrpc import Proxy
from terane.commands.console.results import ResultsListbox
from terane.commands.console.ui import ui, useMainThread
from terane.loggers import getLogger

logger = getLogger('terane.commands.console.search')

class Searcher(urwid.WidgetWrap):
    def __init__(self, host, query):
        self.title = query
        self._host = host
        self._query = query
        self._results = ResultsListbox()
        self._frame = urwid.Frame(self._results)
        # make the search request
        proxy = Proxy("http://%s/XMLRPC" % self._host, allowNone=True)
        deferred = proxy.callRemote('search', self._query)
        deferred.addCallback(self._getResult)
        deferred.addErrback(self._getError)
        urwid.WidgetWrap.__init__(self, self._frame)

    @useMainThread
    def _getResult(self, results):
        self._meta = results.pop(0)
        for r in results:
            self._results.append(r)
        # redraw the listbox widget
        ui.redraw()

    @useMainThread
    def _getError(self, failure):
        try:
            raise failure.value
        except Fault, e:
            errtext = "search failed: %s (code %i)" % (e.faultString,e.faultCode)
            logger.debug(errtext)
            self._frame.set_body(urwid.Filler(urwid.Text(errtext, align='center')))
        except BaseException, e:
            errtext = "search failed: %s" % str(e)
            logger.debug(errtext)
            self._frame.set_body(urwid.Filler(urwid.Text(errtext, align='center')))
        ui.redraw()

    def command(self, cmd, args):
        if self._results != None:
            return self._results.command(cmd, args)
        return None
