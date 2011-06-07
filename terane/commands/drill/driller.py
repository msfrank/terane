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
from logging import StreamHandler, DEBUG, Formatter
from twisted.internet import reactor
from terane.commands.drill.screen import Screen
from terane.commands.drill.search import Searcher
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
        ev = urwid.TwistedEventLoop(reactor=reactor)
        screen = Screen()
        loop = urwid.MainLoop(screen, unhandled_input=self.unhandled_input, event_loop=ev)
        if self.query != '':
            searcher = Searcher(loop, self.host, self.query)
            screen.setWindow(searcher)
        loop.run()
        logger.debug("exited urwid main loop")
        if self.debug == True:
            handler = StreamHandler()
            handler.setLevel(DEBUG)
            handler.setFormatter(Formatter("%(asctime)s %(levelname)s: %(message)s"))
            startLogging(handler)
        else:
            startLogging(None)
        return 0

    def unhandled_input(self, unhandled):
        logger.debug("caught unhandled input '%s'" % str(unhandled))
