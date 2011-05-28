import os, sys, curses
from curses import ascii
from twisted.internet import reactor
from terane.loggers import getLogger

logger = getLogger('terane.commands.drill.input')

class ViewMode(object):
    def __init__(self, input):
        logger.debug("switched to view mode")
        self._input = input

    def pos(self):
        return 0

    def draw(self):
        self._input._win.erase()
        self._input._win.refresh()

    def process(self, ch):
        if ch == ord(':'):
            self._input._mode = CommandMode(self._input)
            self._input._mode.draw()
        elif ch == ord('k'):
            logger.debug("scroll window up")
        elif ch == ord('j'):
            logger.debug("scroll window down")
        elif ch == ord('c'):
            logger.debug("toggle collapsed")
        elif ch == ord('q'):
            reactor.stop()
        return None

class CommandMode(object):
    def __init__(self, input):
        logger.debug("switched to command mode")
        self._input = input
        self._pad = curses.newpad(1, self._input.width)
        self._pad.addch(0, 0, ord(':'))
        self._pos = 1
        self._offset = 0

    def pos(self):
        return self._pos

    def draw(self):
        self._input._win.erase()
        self._input._win.move(0, self._pos)
        self._pad.refresh(0, self._offset,
            self._input.y, self._input.x,
            self._input.y + self._input.height, self._input.x + self._input.width
            )

    def _docommand(self):
        line = self._pad.instr(0, 1).strip()
        logger.debug("processing command: '%s'" % line)
        self._input._mode = ViewMode(self._input)
        self._input._mode.draw()

    def _backspace(self):
        return
        if self._pos <= 1:
            return
        self._pos -= 1
        self._input._win.delch(0, self._pos)
        self._input._win.move(0, self._pos)
        self._input._win.refresh()

    def process(self, ch):
        if ch == ascii.ESC:
            self._input._mode = ViewMode(self._input)
            self._input._mode.draw()
        elif ch == ascii.BS or ch == ascii.DEL:
            self._backspace()
        elif ch == ascii.LF or ch == ascii.CR:
            self._docommand()
        elif ascii.isprint(ch):
            self._pad.addch(0, self._offset + self._pos, ch)
            self._pos += 1
            self.draw()
        return None

class GetchError(Exception):
    pass

class ResizeScreen(Exception):
    pass

class Input(object):
    def __init__(self):
        self._win = None
        self._mode = ViewMode(self)
        self.x = 0
        self.y = 0
        self.width = 0
        self.height = 0

    def refresh(self, y, x, height, width):
        self.x = x
        self.y = y
        if width != self.width:
            self.width = width
        self._win = curses.newwin(self.height, self.width, self.y, self.x)
        self._mode.draw()

    def process(self):
        ch = self._win.getch(0, self._mode.pos())
        if ch == -1:
            raise GetchError()
        if ch == curses.KEY_RESIZE:
            raise ResizeScreen()
        return self._mode.process(ch)
