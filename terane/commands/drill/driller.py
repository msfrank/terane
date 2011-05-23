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

import os, sys, curses
from logging import StreamHandler, DEBUG, Formatter
from twisted.internet import reactor
from terane.commands.drill.input import Input
from terane.commands.drill.output import Output
from terane.loggers import startLogging

class Driller(object):
    def configure(self, settings):
        # load configuration
        section = settings.section("search")
        self.host = section.getString("host", 'localhost:7080')
        self.debug = section.getBoolean("debug", False)
        self.file = settings.args()[0]

    def run(self):
        try:
            stdscr = curses.initscr()
            curses.cbreak()
            curses.noecho()
            stdscr.keypad(1)
            try:
                curses.start_color()
            except:
                pass
            output = Output(stdscr)
            with file(self.file, 'r') as f:
                output.append(f.readlines())
            input = Input(stdscr, output)
            reactor.addReader(input)
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
