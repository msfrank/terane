import os, sys, curses
from curses import ascii
from twisted.internet import reactor
from terane.loggers import getLogger

logger = getLogger('terane.commands.drill.input')

class ViewMode(object):
    def __init__(self, input):
        self._input = input
        self._modechange = False
        input._inwin.erase()
        input._inwin.refresh()
        logger.debug("switched to view mode")

    def process(self, ch):
        if self._modechange:
            if ch == ord(':'):
                self._input._mode = CommandMode(self._input)
            else:
                self._modechange = False
        else:
            if ch == ascii.ESC:
                self._modechange = True
            elif ch == ord('k'):
                self._input._output.up()
            elif ch == ord('j'):
                self._input._output.down()
            elif ch == ord('q'):
                reactor.stop()

class CommandMode(object):
    def __init__(self, input):
        self._input = input
        self._modechange = False
        self._input._inwin.erase()
        self._input._inwin.addch(0, 0, ord(':'))
        self._pos = 1
        self._input._inwin.move(0, self._pos)
        self._input._inwin.refresh()
        logger.debug("switched to command mode")

    def _docommand(self):
        line = self._input._inwin.instr(0, 1).strip()
        logger.debug("processing command: '%s'" % line)
        self._input._mode = ViewMode(self._input)

    def _backspace(self):
        if self._pos <= 1:
            return
        self._pos -= 1
        self._input._inwin.delch(0, self._pos)
        self._input._inwin.move(0, self._pos)
        self._input._inwin.refresh()

    def process(self, ch):
        if ch == ascii.ESC:
            self._input._mode = ViewMode(self._input)
        elif ch == ascii.BS or ch == ascii.DEL:
            self._backspace()
        elif ch == ascii.LF or ch == ascii.CR:
            self._docommand()
        elif ascii.isprint(ch):
            self._input._inwin.addch(0, self._pos, ch)
            self._pos += 1
            self._input._inwin.move(0, self._pos)
            self._input._inwin.refresh()

class Input(object):
    def __init__(self, screen, output):
        screenh,screenw = screen.getmaxyx()
        self._screensize = (screenh,screenw)
        self._inpad = curses.newpad(1, screenw)
        self._inwin = curses.newwin(1, screenw, screenh - 1, 0)
        self._output = output
        self._mode = ViewMode(self)

    def fileno(self):
        return 0

    def doRead(self):
        ch = self._inwin.getch()
        self._mode.process(ch)

    def connectionLost(self, reason):
        pass

    def logPrefix(self):
        return "terane"
