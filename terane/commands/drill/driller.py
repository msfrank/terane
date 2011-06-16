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
from terane.commands.drill.input import Input
from terane.commands.drill.search import Searcher
from terane.commands.drill.outfile import Outfile
from terane.commands.drill.ui import ui
from terane.loggers import startLogging, getLogger

logger = getLogger('terane.commands.drill.driller')

class Driller(urwid.WidgetWrap):
    def __init__(self):
        self._blank = urwid.SolidFill()
        self._input = Input()
        self._frame = urwid.Frame(self._blank, footer=self._input)
        self._frame.set_focus('footer')
        self._windows = []
        urwid.WidgetWrap.__init__(self, self._frame)

    def configure(self, settings):
        # load configuration
        section = settings.section("drill")
        self.host = section.getString("host", 'localhost:7080')
        self.executecmd = section.getString('execute command', None)
        self.debug = section.getBoolean("debug", False)

    def keypress(self, size, key):
        key = self._input.keypress(size, key)
        if key == None:
            return key
        if key.startswith('command'):
            cmdline = key.split()
            if len(cmdline) < 2:
                return key
            cmd,args = cmdline[1],cmdline[2:]
            return self.command(cmd, args)
        if key != None and len(self._windows) > 0:
            return self._windows[0].keypress(size, key)
        return key

    def command(self, cmd, args):
        # window management commands
        if cmd == 'list':
            pass
        elif cmd == 'prev':
            self.prevWindow()
        elif cmd == 'next':
            self.nextWindow()
        elif cmd == 'close':
            self.closeWindow()
        # actions
        elif cmd == 'search':
            searcher = Searcher(self.host, ' '.join(args))
            self.setWindow(searcher)
        elif cmd == 'load':
            outfile = Outfile(args[0])
            self.setWindow(outfile)
        # forward other commands to the active window
        elif len(self._windows) > 0:
            return self._windows[0].command(cmd, args)
        return None

    def mouse_event(self, size, event, button, col, row, focus):
        """
        Ignore mouse events.
        """
        pass

    def setWindow(self, window):
        """
        Add the specified window to the window list and bring it to the front.
        """
        self._windows.insert(0, window)
        self._frame.set_body(window)
        self._frame.set_focus('footer')

    def nextWindow(self):
        try:
            window = self._windows.pop(0)
        except IndexError:
            return
        self._windows.append(window)
        self._frame.set_body(window)
        self._frame.set_focus('footer')

    def prevWindow(self):
        try:
            window = self._windows.pop(-1)
        except IndexError:
            return
        self._windows.insert(0, window)
        self._frame.set_body(window)
        self._frame.set_focus('footer')

    def closeWindow(self):
        try:
            self._windows.pop(0)
        except IndexError:
            return
        if len(self._windows) > 0:
            window = self._windows[0]
        else:
            window = self._blank
        self._frame.set_body(window)
        self._frame.set_focus('footer')

    def run(self):
        """
        Start the event loop.
        """
        ui.setroot(self)
        if self.executecmd != None:
            cmdline = self.executecmd.split()
            self.command(cmdline[0], cmdline[1:])
        ui.run()
        logger.debug("exited urwid main loop")
        if self.debug == True:
            handler = StreamHandler()
            handler.setLevel(DEBUG)
            handler.setFormatter(Formatter("%(asctime)s %(levelname)s: %(message)s"))
            startLogging(handler)
        else:
            startLogging(None)
        return 0
