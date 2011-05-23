import os, sys, curses
from terane.loggers import getLogger

logger = getLogger('terane.commands.drill.output')

class Output(object):
    def __init__(self, screen):
        self._screen = screen
        self._lines = []
        self._pos = 0
        # allocate the sub-pads
        h,w = screen.getmaxyx()
        logger.debug("screen is %i x %i" % (w, h))
        self._screensize = (h, w)
        self._outpad = None
        self._outsize = (0, 0)

    def append(self, lines):
        self._lines += [l.rstrip() for l in lines]
        self._refresh()
        logger.debug("appended %i lines" % len(lines))

    def _refresh(self):
        screenh,screenw = self._screensize
        # calculate the height of the output
        lineh = 0
        for l in self._lines:
            if l == '':
                lineh += 1
            else:
                q,r = divmod(len(l), screenw)
                lineh += q
                if r > 0: lineh += 1
        outputh,outputw = self._outsize
        # if the screen width has changed, or the line
        # height exceeds the output window height, then
        # redraw the lines to the output window
        if screenw != outputw or lineh > outputh:
            self._outpad = curses.newpad(lineh, screenw)
            self._outsize = (lineh, screenw)
            outputh,outputw = self._outsize
            logger.debug("output pad is %i x %i" % (outputw, outputh))
            i = 0
            for l in self._lines:
                if l == '':
                    i += 1
                else:
                    self._outpad.addstr(i, 0, l)
                    q,r = divmod(len(l), outputw)
                    i += q
                    if r > 0: i += 1
        # refresh the output window
        self._outpad.refresh(self._pos, 0, 0, 0, screenh - 2, screenw - 2)

    def up(self):
        if self._pos > 0:
            self._pos -= 1
            self._refresh()

    def down(self):
        screenh,screenw = self._screensize
        outputh,outputw = self._outsize
        if self._pos < outputh - screenh:
            self._pos += 1
            self._refresh()
