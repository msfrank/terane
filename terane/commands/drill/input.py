import os, sys, urwid
from twisted.internet import reactor
from terane.loggers import getLogger

logger = getLogger('terane.commands.drill.input')


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
