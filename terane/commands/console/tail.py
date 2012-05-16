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

logger = getLogger('terane.commands.console.tail')

class Tailer(Window):
    def __init__(self, args):
        title = "Tail '%s'" % args
        self._query = args
        self._lastId = None
        self._results = ResultsListbox()
        self.interval = 2
        self._url = "http://%s/XMLRPC" % console.host
        self._user = console.username
        self._pass = console.password
        self._delayed = None
        self._deferred = None
        logger.debug("using proxy url %s" % self._url)
        Window.__init__(self, title, self._results)

    def startService(self):
        Window.startService(self)
        logger.debug("started tail: using query '%s'" % self._query)
        self._proxy = Proxy(self._url, user=self._user, password=self._pass, allowNone=True)
        self._tail()

    def stopService(self):
        if self._delayed != None:
            self._delayed.cancel()
        self._delayed = None
        if self._deferred != None:
            self._deferred.cancel()
        self._deferred = None
        logger.debug("stopped tail")
        Window.stopService(self)

    def _tail(self):
        self._deferred = self._proxy.callRemote('tail', self._query, self._lastId)
        self._deferred.addCallback(self._getResult)
        self._deferred.addErrback(self._getError)

    @useMainThread
    def _getResult(self, results):
        """
        Append each search result into the ResultsListbox.
        """
        try:
            meta = results.pop(0)
            self._lastId = meta['last-id']
            logger.debug("tail returned %i results, last id is %s" % (len(results), self._lastId))
            if len(results) > 0:
                for evid,event in results:
                    self._results.append(EVID.fromString(evid), event)
                console.redraw()
            from twisted.internet import reactor
            self._delayed = reactor.callLater(self.interval, self._tail)
        except Exception, e:
            logger.exception(e)

    @useMainThread
    def _getError(self, failure):
        """
        Display the error popup.
        """
        try:
            self._delayed = None
            # close the window
            console.switcher.closeWindow(console.switcher.findWindow(self))
            # display the error on screen
            raise failure.value
        except Fault, e:
            errtext = "Tail failed: %s (code %i)" % (e.faultString,e.faultCode)
        except ValueError, e:
            errtext = "Tail failed: remote server returned HTTP status %s: %s" % e.args
        except BaseException, e:
            errtext = "Tail failed: %s" % str(e)
        console.error(errtext)
        logger.debug(errtext)

    def pause(self):
        if self._delayed == None and self._deferred == None:
            return
        if self._delayed != None:
            self._delayed.cancel()
        self._delayed = None
        if self._deferred != None:
            self._deferred.cancel()
        self._deferred = None
        logger.debug("paused tail")

    def resume(self):
        if self._delayed != None or self._deferred != None:
            return
        self._tail()
        logger.debug("resumed tail")

    def command(self, cmd, args):
        if cmd == 'pause':
            return self.pause()
        if cmd == 'resume':
            return self.resume()
        if self._results != None:
            return self._results.command(cmd, args)
        return None

    def setvar(self, name, value):
        self._results.setvar(name, value)

    def redraw(self):
        self._results.redraw()
