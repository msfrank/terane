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
        self._windows = WindowSwitcher(self._frame)
        urwid.WidgetWrap.__init__(self, self._frame)

    def configure(self, settings):
        # load configuration
        section = settings.section("console")
        self.host = section.getString("host", 'localhost:45565')
        self.executecmd = section.getString('execute command', None)
        self.debug = section.getBoolean("debug", False)

    def keypress(self, size, key):
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
        if key != None and len(self._windows) > 0:
            return self._windows[0].keypress(size, key)
        return key

    def command(self, cmd, args):
        logger.debug("command=%s, args='%s'" % (cmd, args))
        # window management commands
        if cmd == 'windows':
            self._windows.showWindowlist()
        elif cmd == 'prev':
            self._windows.prevWindow()
        elif cmd == 'next':
            self._windows.nextWindow()
        elif cmd == 'jump':
            self._windows.jumpToWindow(args)
        elif cmd == 'close':
            self._windows.closeWindow()
        # actions
        elif cmd == 'search':
            searcher = Searcher(self.host, args)
            self._windows.addWindow(searcher)
        elif cmd == 'load':
            outfile = Outfile(args)
            self._windows.addWindow(outfile)
        elif cmd == 'quit':
            reactor.stop()
        # forward other commands to the active window
        elif len(self._windows) > 0:
            return self._windows[0].command(cmd, args)
        return None

    def mouse_event(self, size, event, button, col, row, focus):
        """
        Ignore mouse events.
        """
        pass

    def run(self):
        """
        Start the event loop.
        """
        ui.setroot(self)
        if self.executecmd != None:
            cmdline = self.executecmd.split(None, 1)
            self.command(cmdline[0], cmdline[1])
        ui.run()
        logger.debug("exited urwid main loop")
        if self.debug == True:
            startLogging(StdoutHandler(), DEBUG)
        return 0

class WindowSwitcher(urwid.WidgetWrap, urwid.ListWalker):
    def __init__(self, frame):
        self._frame = frame
        self._windows = []
        self._nextid = 1
        self._blank = urwid.SolidFill()
        self._currwin = None
        self._currpos = None
        # build the listbox widget
        self._windowlist = urwid.ListBox(self)
        urwid.WidgetWrap.__init__(self, self._windowlist)

    def __len__(self):
        return len(self._windows)

    def __getitem__(self, i):
        return self._windows[i][0]

    def _get_item(self, position):
        return urwid.Text(self._windows[position][2])

    def get_focus(self):
        if self._currpos == None:
            return (None,None)
        return (self._get_item(self._currpos), self._currpos)

    def set_focus(self, position):
        if position < 0 or position >= len(self._windows) - 1:
            return
        self._currpos = position
        urwid.ListWalker._modified(self)

    def get_prev(self, position):
        if position == None or position < 1:
            return (None,None)
        position = position - 1
        item = self._get_item(position)
        return (item, position)

    def get_next(self, position):
        if position == None or position >= len(self._windows) - 1:
            return (None,None)
        position = position + 1
        item = self._get_item(position)
        return (item, position)

    def showWindowlist(self):
        window = self._frame.get_body()
        if window == self:
            return
        if len(self._windows) < 1:
            self._currpos = None
        else:
            self._currpos = 0
        self._frame.set_body(self)
        self._frame.set_focus('footer')

    def addWindow(self, window):
        """
        Add the specified window to the window list and bring it to the front.
        """
        try:
            desc = "Window #%i - %s" % (self._nextid, getattr(window, 'title'))
        except:
            desc = "Window #%i" % self._nextid
        self._windows.append((window,self._nextid,desc))
        self._nextid += 1
        self._frame.set_body(window)
        self._frame.set_focus('footer')
        self._currwin = len(self._windows) - 1

    def nextWindow(self):
        window = self._frame.get_body()
        if window == self:
            return self.closeWindow()
        if len(self._windows) <= 1:
            return
        if self._currwin == len(self._windows) - 1:
            self._currwin = 0
        else:
            self._currwin += 1
        window = self._windows[self._currwin][0]
        self._frame.set_body(window)
        self._frame.set_focus('footer')

    def prevWindow(self):
        window = self._frame.get_body()
        if window == self:
            return self.closeWindow()
        if len(self._windows) <= 1:
            return
        if self._currwin == 0:
            self._currwin = len(self._windows) - 1
        else:
            self._currwin -= 1
        window = self._windows[self._currwin][0]
        self._frame.set_body(window)
        self._frame.set_focus('footer')

    def jumpToWindow(self, args):
        try:
            dest = int(args[0])
        except:
            return
        for i in range(0, len(self._windows)):
            if dest == self._windows[i][1]:
                self._currwin = i
                self._frame.set_body(self._windows[i][0])
                self._frame.set_focus('footer')
                break

    def closeWindow(self):
        window = self._frame.get_body()
        # there are no windows open
        if window == self._blank:
            return
        # the current window is the window list
        if window == self:
            if self._currwin == None:
                window = self._blank
            else:
                window = self._windows[self._currwin][0]
        else:
            self._windows.pop(self._currwin)
            self._currwin -= 1
            window = self._windows[self._currwin][0]
        # display the new active window   
        self._frame.set_body(window)
        self._frame.set_focus('footer')
