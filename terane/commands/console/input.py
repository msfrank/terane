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
from twisted.internet import reactor
from terane.loggers import getLogger

logger = getLogger('terane.commands.console.input')


class Input(urwid.FlowWidget):
    def __init__(self):
        # if _buffer is None, we are in view mode
        self._buffer = None
        self._offset = 0

    def selectable(self):
        return True

    def rows(self, size, focus=False):
        return 1

    def render(self, size, focus=False):
        (maxcol,) = size
        cursor = None
        if focus:
            cursor = self.get_cursor_coords(size)
        if self._buffer == None:
            return urwid.TextCanvas([' '], maxcol=maxcol, cursor=cursor)
        return urwid.TextCanvas([':' + ''.join(self._buffer)], maxcol=maxcol, cursor=cursor)

    def keypress(self, size, key):
        # ignore window resize event
        if key == 'window resize':
            return None
        if self._buffer == None:
            if key == ':':
                self._buffer = []
                key = None
            elif key == 'q':
                reactor.stop()
                key = None
        else:
            if key == 'esc':
                self._buffer = None
                key = None
            elif key == 'backspace' or key == 'delete':
                if len(self._buffer) > 0:
                    self._buffer.pop()
                key = None
            elif key == 'enter' or key == 'return':
                line = ''.join(self._buffer)
                self._buffer = None
                line = line.strip()
                key = "command %s" % line
            elif len(key) == 1:
                self._buffer.append(key)
                key = None
        self._invalidate()
        return key


    def get_cursor_coords(self, size):
        if self._buffer == None:
            return (0, 0)
        return (1 + len(self._buffer), 0)
