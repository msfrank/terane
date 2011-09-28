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
from twisted.web.xmlrpc import Proxy
from terane.commands.console.results import ResultsListbox
from terane.commands.console.ui import ui, useMainThread
from terane.loggers import getLogger

logger = getLogger('terane.commands.console.search')

class Searcher(urwid.WidgetWrap):
    def __init__(self, console, args):
        # configure the searcher
        self._console = console
        self.title = "Search results for '%s'" % args
        self._query = args
        self._results = ResultsListbox()
        url = "http://%s/XMLRPC" % console.host
        logger.debug("using proxy url %s" % url)
        # make the xmlrpc search request
        proxy = Proxy(url, allowNone=True)
        deferred = proxy.callRemote('search', self._query)
        deferred.addCallback(self._getResult)
        deferred.addErrback(self._getError)
        urwid.WidgetWrap.__init__(self, self._results)

    @useMainThread
    def _getResult(self, results):
        """
        Append each search result into the ResultsListbox.
        """
        self._meta = results.pop(0)
        for r in results:
            self._results.append(r)
        ui.redraw()

    @useMainThread
    def _getError(self, failure):
        """
        Display the error popup.
        """
        # close the search window
        self._console.switcher.closeWindow(self.console.switcher.findWindow(self))
        # display the error on screen
        try:
            raise failure.value
        except Fault, e:
            errtext = "Search failed: %s (code %i)" % (e.faultString,e.faultCode)
            ui.error(errtext)
            logger.debug(errtext)
        except BaseException, e:
            errtext = "Search failed: %s" % str(e)
            ui.error(errtext)
            logger.debug(errtext)

    def command(self, cmd, args):
        if self._results != None:
            return self._results.command(cmd, args)
        return None
