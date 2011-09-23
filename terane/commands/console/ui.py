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

import os, sys, urwid, functools
from twisted.internet import reactor
from terane.loggers import getLogger

logger = getLogger('terane.commands.console.ui')

class Error(urwid.WidgetWrap):
    def __init__(self, exception):
        self._text = urwid.Text([('bold',str(exception)), '\nPress any key to continue'])
        self._frame = urwid.Frame(urwid.SolidFill(), footer=self._text)
        self._frame.set_focus('footer')
        urwid.WidgetWrap.__init__(self, self._frame)

    def keypress(self, size, key):
        ui._ui.set_body(ui._root)

class UIManager(object):
    def __init__(self):
        blank = urwid.SolidFill()
        self._ui = urwid.Frame(blank)
        self._root = None
        self._loop = None
        self._palette = [
            ('normal', 'default', 'default'),
            ('highlight', 'standout', 'default'),
            ('bold', 'bold', 'default'),
            ]

    def run(self, root):
        self._root = root
        self._ui.set_body(self._root)
        ev = urwid.TwistedEventLoop(reactor=reactor)
        self._loop = urwid.MainLoop(self._ui, 
            palette=self._palette,
            unhandled_input=self._unhandled_input,
            event_loop=ev)
        return self._loop.run()

    def quit(self):
        if self._loop != None: reactor.stop()

    def redraw(self):
        if self._loop != None: self._loop.draw_screen()

    def error(self, exception):
        self._ui.set_body(Error(exception))
        self.redraw()

    def _unhandled_input(self, unhandled):
        logger.debug("caught unhandled input '%s'" % str(unhandled))

ui = UIManager()
    
def useMainThread(fn):
    """
    A decorator for methods which should be run in a separate thread.
    """
    @functools.wraps(fn)
    def _threadWrapper(*args, **kwds):
        return reactor.callFromThread(fn, *args, **kwds)
    return _threadWrapper
