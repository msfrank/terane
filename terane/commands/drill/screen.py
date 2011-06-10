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
from terane.commands.drill.input import Input
from terane.loggers import getLogger

logger = getLogger('terane.commands.drill.screen')

class Screen(urwid.WidgetWrap):
    def __init__(self):
        self._blank = urwid.SolidFill()
        self._input = Input()
        self._frame = urwid.Frame(self._blank, footer=self._input)
        self._frame.set_focus('footer')
        urwid.WidgetWrap.__init__(self, self._frame)
        self._windows = []

    def keypress(self, size, key):
        key = self._input.keypress(size, key)
        if key != None and len(self._windows) > 0:
            return self._windows[0].keypress(size, key)
        return key

    def mouse_event(self, size, event, button, col, row, focus):
        pass

    def setWindow(self, window):
        self._windows = [window,]
        self._frame.set_body(window)
        self._frame.set_focus('footer')
