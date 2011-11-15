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

import os, sys, urwid, dateutil.tz
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
            ('black', 'black', 'default'),
            ('dark-red', 'dark red', 'default'),
            ('dark-green', 'dark green', 'default'),
            ('brown', 'brown', 'default'),
            ('dark-blue', 'dark blue', 'default'),
            ('dark-magenta', 'dark magenta', 'default'),
            ('dark-cyan', 'dark cyan', 'default'),
            ('light-gray', 'light gray', 'default'),
            ('dark-gray', 'dark gray', 'default'),
            ('light-red', 'light red', 'default'),
            ('light-green', 'light green', 'default'),
            ('yellow', 'yellow', 'default'),
            ('light-blue',  'light blue', 'default'),
            ('light-magenta', 'light magenta', 'default'),
            ('light-cyan', 'light cyan', 'default'),
            ('white', 'white', 'default'),
            ]
        self.palette = {
            'text': 'normal',
            'highlight': 'highlight',
            'date': 'normal',
            'field-name': 'normal',
            'field-value': 'normal'
            }
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
        self.tz = section.getString("timezone", None)
        if self.tz != None:
            self.tz = dateutil.tz.gettz(self.tz)
        self.executecmd = section.getString('execute command', None)
        self.debug = section.getBoolean("debug", False)
        self.logconfigfile = section.getString('log config file', "%s.logconfig" % settings.appname)
        # load external palette, if specified
        palettefile = section.getPath('palette file', None)
        if palettefile and os.access(palettefile, os.R_OK):
            with open(palettefile, 'r') as f:
                logger.debug("loading palette from %s" % palettefile)
                for line in f.readlines():
                    line = line.strip()
                    # ignore blank lines and lines beginning with '#'
                    if line == '' or line.startswith('#'):
                        continue
                    spec = [s.strip() for s in line.split(',')]
                    if len(spec) < 3:
                        continue
                    self._palette.append(spec[0:5])
                    logger.debug("loaded palette entry '%s': %s" % (spec[0], ', '.join(spec[1:5])))
        # set palette entries
        self.palette['text'] = section.getString('text color', self.palette['text'])
        self.palette['highlight'] = section.getString('highlight color', self.palette['highlight'])
        self.palette['date'] = section.getString('date color', self.palette['date'])
        self.palette['field-name'] = section.getString('field name color', self.palette['field-name'])
        self.palette['field-value'] = section.getString('field value color', self.palette['field-value'])

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
        if cmd == 'set':
            name,value = [v.strip() for v in args.strip().split(' ', 1)]
            return self.setvar(name, value)
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

    def setvar(self, name, value):
        """
        Parse runtime variable set command.
        """
        if name == 'default-host':
            logger.debug("set %s = %s" % (name, value))
            self.host = value
        elif name == 'default-timezone':
            tz = dateutil.tz.gettz(value)
            if tz != None:
                self.tz = tz
                logger.debug("set %s = %s" % (name, value))
        elif name == 'text-color':
            self.palette['text'] = value
            logger.debug("set %s = %s" % (name, value))
            self.redraw()
        elif name == 'highlight-color':
            self.palette['highlight'] = value
            logger.debug("set %s = %s" % (name, value))
            self.redraw()
        elif name == 'date-color':
            self.palette['date'] = value
            logger.debug("set %s = %s" % (name, value))
            self.redraw()
        elif name == 'field-name-color':
            self.palette['field-name'] = value
            logger.debug("set %s = %s" % (name, value))
            self.redraw()
        elif name == 'field-value-color':
            self.palette['field-value'] = value
            logger.debug("set %s = %s" % (name, value))
            self.redraw()
        else:
            self.switcher.setvar(name, value)

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
        reactor.callLater(0, self.startService)
        self._loop.run()
        logger.debug("exited urwid main loop")
        if self.debug == True:
            startLogging(StdoutHandler(), DEBUG, self.logconfigfile)
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
        if self._loop == None:
            return
        self.switcher.redraw()
        self._input.redraw()
        self._loop.draw_screen()

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
