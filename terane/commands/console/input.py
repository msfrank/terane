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

    VIEW_MODE = 0
    COMMAND_MODE = 1
    FIND_MODE = 2
    RFIND_MODE = 3

    def __init__(self):
        self._buffer = None
        self._offset = 0
        self._mode = Input.VIEW_MODE

    def selectable(self):
        """
        The Input widget accepts user input.
        """
        return True

    def rows(self, size, focus=False):
        """
        The Input widget is 1 row high.
        """
        return 1

    def render(self, size, focus=False):
        """
        Display the Input widget.  The input is properly scrolled if there is
        too much data to display on the screen all at once.
        """
        (maxcol,) = size
        cursor = None
        # we are in view mode, so display a blank row
        if self._mode == Input.VIEW_MODE:
            if focus:
                cursor = self.get_cursor_coords(size)
            return urwid.TextCanvas([' '], maxcol=maxcol, cursor=cursor)
        # if the size of the buffer starting from offset is larger than the
        # screen width, then scroll the input left by half a screen.
        if len(self._buffer) - self._offset + 1 >= maxcol:
            self._offset += maxcol / 2
        # if the offset is larger than the size of the buffer, then scroll the
        # input right by a whole screen, minus 1 char for the cursor.
        if len(self._buffer) == self._offset - 1:
            self._offset -= maxcol - 1
            if self._offset < 0: self._offset = 0
        # calculate the visible characters in the buffer
        visible = ''.join(self._buffer[self._offset:])
        # display the input line
        if focus:
            cursor = self.get_cursor_coords(size)
        if self._mode == Input.COMMAND_MODE:
            indicator = ':'
        if self._mode == Input.FIND_MODE:
            indicator = '/'
        if self._mode == Input.RFIND_MODE:
            indicator = '?'
        return urwid.TextCanvas([indicator + visible], maxcol=maxcol, cursor=cursor)

    def keypress(self, size, key):
        """
        Process input events.
        """
        # if the window is resized, invalidate the widget
        if key == 'window resize':
            self._invalidate()
            return None
        # if _we are in view mode
        if self._mode == Input.VIEW_MODE:
            if key == ':':
                self._buffer = []
                self._offset = 0
                self._mode = Input.COMMAND_MODE
                key = None
            elif key == '/':
                self._buffer = []
                self._offset = 0
                self._mode = Input.FIND_MODE
                key = None
            elif key == '?':
                self._buffer = []
                self._offset = 0
                self._mode = Input.RFIND_MODE
                key = None
        # otherwise we are in one of the command input modes (COMMAND, FIND, 
        # RFIND).  in this mode, backspace or delete removes the last
        # character in the input buffer, enter or return executes the command,
        # esc switches back to view mode, and any other single character is
        # considered input.
        else:
            if key == 'esc':
                self._buffer = None
                self._offset = 0
                self._mode = Input.VIEW_MODE
                key = None
            elif key == 'backspace' or key == 'delete':
                if len(self._buffer) > 0:
                    self._buffer.pop()
                key = None
            elif key == 'enter' or key == 'return':
                line = ''.join(self._buffer)
                self._buffer = None
                self._offset = 0
                line = line.strip()
                if self._mode == Input.COMMAND_MODE and len(line) > 0:
                    key = "command %s" % line
                elif self._mode == Input.FIND_MODE:
                    key = "command find /%s" % line
                elif self._mode == Input.RFIND_MODE:
                    key = "command rfind ?%s" % line
                self._mode = Input.VIEW_MODE
            elif len(key) == 1:
                self._buffer.append(key)
                key = None
        self._invalidate()
        return key


    def get_cursor_coords(self, size):
        """
        Return the cursor coordinates as a tuple.
        """
        if self._buffer == None:
            return (0, 0)
        return (1 + len(self._buffer) - self._offset, 0)
