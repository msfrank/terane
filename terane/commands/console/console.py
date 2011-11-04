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
from twisted.application.service import MultiService
from twisted.internet import reactor
from twisted.internet.defer import maybeDeferred
from terane.commands.console.switcher import WindowSwitcher
from terane.commands.console.input import Input
from terane.loggers import getLogger, startLogging, StdoutHandler, DEBUG

logger = getLogger('terane.commands.console.console')

class Console(MultiService, urwid.WidgetWrap):
    def __init__(self):
        MultiService.__init__(self)
        self._loop = None
        self._palette = [
            ('normal', 'default', 'default'),
            ('highlight', 'standout', 'default'),
            ('bold', 'bold', 'default'),
            ]
        self._ui = urwid.Frame(urwid.SolidFill())
        self._input = Input()
        self._frame = urwid.Frame(urwid.SolidFill(), footer=self._input)
        self._frame.set_focus('footer')
        self.switcher = WindowSwitcher(self._frame)
        self.addService(self.switcher)
        urwid.WidgetWrap.__init__(self, self._frame)

    def configure(self, settings):
        # load configuration
        section = settings.section("console")
        self.host = section.getString("host", 'localhost:45565')
        self.executecmd = section.getString('execute command', None)
        self.debug = section.getBoolean("debug", False)

    def startService(self):
        MultiService.startService(self)
        logger.debug("started console service")

    def stopService(self):
        MultiService.stopService(self)
        logger.debug("stopped console service")

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
            from terane.commands.console.search import Searcher
            searcher = Searcher(args)
            return self.switcher.addWindow(searcher)
        if cmd == 'tail':
            from terane.commands.console.tail import Tailer
            tailer = Tailer(args)
            return self.switcher.addWindow(tailer)
        if cmd == 'load':
            from terane.commands.console.outfile import Outfile
            outfile = Outfile(args)
            return self.switcher.addWindow(outfile)
        if cmd == 'quit':
            return self.quit()
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
        self._ui.set_body(self)
        ev = urwid.TwistedEventLoop(reactor=reactor)
        self._loop = urwid.MainLoop(self._ui, 
            palette=self._palette,
            unhandled_input=self._unhandled_input,
            event_loop=ev)
        self.startService()
        self._loop.run()
        logger.debug("exited urwid main loop")
        if self.debug == True:
            startLogging(StdoutHandler(), DEBUG)
        return 0

    def _unhandled_input(self, unhandled):
        logger.debug("caught unhandled input '%s'" % str(unhandled))

    def quit(self):
        if not reactor.running or not self.running:
            return
        d = maybeDeferred(self.stopService)
        d.addCallback(self._quit)

    def _quit(self, result):
        if self._loop != None: reactor.stop()

    def redraw(self):
        if self._loop != None: self._loop.draw_screen()

    def error(self, exception):
        self._ui.set_body(Error(exception))
        self.redraw()

class Error(urwid.WidgetWrap):
    def __init__(self, exception):
        self._text = urwid.Text([('bold',str(exception)), '\nPress any key to continue'])
        self._frame = urwid.Frame(urwid.SolidFill(), footer=self._text)
        self._frame.set_focus('footer')
        urwid.WidgetWrap.__init__(self, self._frame)

    def keypress(self, size, key):
        console._ui.set_body(console)


console = Console()
