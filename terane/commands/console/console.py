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
from terane.commands.console.switcher import WindowSwitcher
from terane.commands.console.input import Input
from terane.commands.console.search import Searcher
from terane.commands.console.outfile import Outfile
from terane.commands.console.ui import ui
from terane.loggers import getLogger, startLogging, StdoutHandler, DEBUG

logger = getLogger('terane.commands.console.console')

class Console(urwid.WidgetWrap):
    def __init__(self):
        self._input = Input()
        self._frame = urwid.Frame(urwid.SolidFill(), footer=self._input)
        self._frame.set_focus('footer')
        self.switcher = WindowSwitcher(self._frame)
        urwid.WidgetWrap.__init__(self, self._frame)

    def configure(self, settings):
        # load configuration
        section = settings.section("console")
        self.host = section.getString("host", 'localhost:45565')
        self.executecmd = section.getString('execute command', None)
        self.debug = section.getBoolean("debug", False)

    def keypress(self, size, key):
        """
        Parse raw keypresses from urwid.
        """
        key = self._input.keypress(size, key)
        if key == None:
            return key
        if key.startswith('command'):
            try:
                cmdline = key.split(None, 1)[1]
            except:
                return key
            try:
                (cmd,args) = cmdline.split(None, 1)
            except:
                (cmd,args) = (cmdline, '')
            return self.command(cmd, args)
        return self.switcher.keypress(size, key)

    def command(self, cmd, args):
        """
        Parse input commands.
        """
        logger.debug("command=%s, args='%s'" % (cmd, args))
        if cmd == 'search':
            searcher = Searcher(self, args)
            return self.switcher.addWindow(searcher)
        if cmd == 'load':
            outfile = Outfile(self, args)
            return self.switcher.addWindow(outfile)
        if cmd == 'quit':
            return reactor.stop()
        # forward other commands to the active window
        return self.switcher.command(cmd, args)

    def mouse_event(self, size, event, button, col, row, focus):
        """
        Ignore mouse events.
        """
        pass

    def run(self):
        """
        Start the event loop.
        """
        if self.executecmd != None:
            cmdline = self.executecmd.split(None, 1)
            self.command(cmdline[0], cmdline[1])
        ui.run(self)
        logger.debug("exited urwid main loop")
        if self.debug == True:
            startLogging(StdoutHandler(), DEBUG)
        return 0
