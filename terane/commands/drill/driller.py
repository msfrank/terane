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

import os, sys, curses, xmlrpclib
from logging import StreamHandler, DEBUG, Formatter
from twisted.internet import reactor
from twisted.web.xmlrpc import Proxy
from terane.commands.drill.input import Input
from terane.commands.drill.output import Output
from terane.loggers import startLogging, getLogger

logger = getLogger('terane.commands.drill.driller')

class Driller(object):
    def configure(self, settings):
        # load configuration
        section = settings.section("drill")
        self.host = section.getString("host", 'localhost:7080')
        self.debug = section.getBoolean("debug", False)
        self.query = ' '.join(settings.args())

    def run(self):
        try:
            stdscr = curses.initscr()
            curses.cbreak()
            curses.noecho()
            stdscr.keypad(1)
            self._output = Output(stdscr)
            self._input = Input(stdscr, self._output)
            reactor.addReader(self._input)
            proxy = Proxy("http://%s/XMLRPC" % self.host, allowNone=True)
            deferred = proxy.callRemote('search', self.query)
            deferred.addCallback(self.printResult)
            deferred.addErrback(self.printError)
            reactor.run()
        finally:
            stdscr.keypad(0)
            curses.echo()
            curses.nocbreak()
            curses.endwin()
        if self.debug == True:
            handler = StreamHandler()
            handler.setLevel(DEBUG)
            handler.setFormatter(Formatter("%(asctime)s %(levelname)s: %(message)s"))
            startLogging(handler)
        else:
            startLogging(None)
        return 0
        
    def printResult(self, results):
        meta = results.pop(0)
        if len(results) > 0:
            for doc in results:
                self._output.append(doc)

    def printError(self, failure):
        try:
            raise failure.value
        except xmlrpclib.Fault, e:
            logger.debug("search failed: %s (code %i)" % (e.faultString,e.faultCode))
        except BaseException, e:
            logger.debug("search failed: %s" % str(e))
