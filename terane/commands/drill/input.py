import os, sys, urwid
from twisted.internet import reactor
from terane.loggers import getLogger

logger = getLogger('terane.commands.drill.input')

class ModeChanged(BaseException):
    def __init__(self, mode):
        self.mode = mode

class ViewMode(object):
    def __init__(self, input):
        logger.debug("switched to view mode")
        self._input = input
        self._input._invalidate()

    def render(self, size, focus):
        (maxcol,) = size
        cursor = None
        if focus:
            cursor = self.get_cursor_coords(size)
        return urwid.TextCanvas([' '], maxcol=maxcol, cursor=cursor)

    def keypress(self, size, key):
        if key == ':':
            raise ModeChanged(CommandMode(self._input))
        elif key == 'q':
            reactor.stop()
            return None
        return key

    def get_cursor_coords(self, size):
        return (0, 0)

class CommandMode(object):
    def __init__(self, input):
        logger.debug("switched to command mode")
        self._input = input
        self._input._invalidate()
        self._buffer = []
        self._offset = 0

    def render(self, size, focus):
        (maxcol,) = size
        cursor = None
        if focus:
            cursor = self.get_cursor_coords(size)
        return urwid.TextCanvas([':' + ''.join(self._buffer)], maxcol=maxcol, cursor=cursor)

    def _docommand(self):
        line = ''.join(self._buffer)
        line = line.strip()
        logger.debug("processing command: '%s'" % line)
        raise ModeChanged(ViewMode(self._input))

    def _backspace(self):
        if len(self._buffer) > 0:
            self._buffer.pop()

    def keypress(self, size, key):
        if key == 'esc':
            raise ModeChanged(ViewMode(self._input))
        elif key == 'backspace' or key == 'delete':
            self._backspace()
        elif key == 'enter' or key == 'return':
            self._docommand()
        elif len(key) == 1:
            self._buffer.append(key)
        self._input._invalidate()

    def get_cursor_coords(self, size):
        (maxcol,) = size
        return (1 + len(self._buffer), 0)

class Input(urwid.FlowWidget):
    def __init__(self):
        self._mode = ViewMode(self)

    def selectable(self):
        return True

    def rows(self, size, focus=False):
        return 1

    def render(self, size, focus=False):
        return self._mode.render(size, focus)

    def keypress(self, size, key):
        # ignore window resize event
        if key == 'window resize':
            return None
        # forward keypress to mode
        try:
            return self._mode.keypress(size, key)
        except ModeChanged, e:
            self._mode = e.mode

    def get_cursor_coords(self, size):
        return self._mode.get_cursor_coords(size)
