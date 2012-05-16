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
from terane.bier.evid import EVID
from terane.commands.console.switcher import Window
from terane.commands.console.results import ResultsListbox
from terane.commands.console.console import console
from terane.commands.console.ui import useMainThread
from terane.loggers import getLogger

logger = getLogger('terane.commands.console.search')

class Searcher(Window):
    def __init__(self, args):
        # configure the searcher
        title = "Search results for '%s'" % args
        self._query = args
        self._results = ResultsListbox()
        self._url = "http://%s/XMLRPC" % console.host
        self._user = console.username
        self._pass = console.password
        self._deferred = None
        Window.__init__(self, title, self._results)

    def startService(self):
        Window.startService(self)
        logger.debug("started search")
        self.reload()

    def stopService(self):
        if self._deferred != None:
            self._deferred.cancel()
        self._deferred = None
        logger.debug("stopped search")
        Window.stopService(self)

    @useMainThread
    def _getResult(self, results):
        """
        Append each search result into the ResultsListbox.
        """
        try:
            self._meta = results.pop(0)
            for evid,event in results:
                self._results.append(EVID.fromString(evid), event)
            console.redraw()
        except Exception, e:
            logger.exception(e)

    @useMainThread
    def _getError(self, failure):
        """
        Display the error popup.
        """
        try:
            # close the search window
            console.switcher.closeWindow(console.switcher.findWindow(self))
            # display the error on screen
            raise failure.value
        except Fault, e:
            errtext = "Search failed: %s (code %i)" % (e.faultString,e.faultCode)
        except ValueError, e:
            errtext = "Search failed: remote server returned HTTP status %s: %s" % e.args
        except BaseException, e:
            errtext = "Search failed: %s" % str(e)
        console.error(errtext)
        logger.debug(errtext)

    def reload(self):
        if self._deferred != None:
            self._deferred.cancel()
        self._results.clear()
        proxy = Proxy(self._url, user=self._user, password=self._pass, allowNone=True)
        self._deferred = proxy.callRemote('iter', self._query)
        self._deferred.addCallback(self._getResult)
        self._deferred.addErrback(self._getError)
        logger.debug("searching with query '%s'" % self._query)

    def command(self, cmd, args):
        if cmd == 'reload':
            return self.reload()
        if self._results != None:
            return self._results.command(cmd, args)
        return None

    def setvar(self, name, value):
        self._results.setvar(name, value)

    def redraw(self):
        self._results.redraw()
