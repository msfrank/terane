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
