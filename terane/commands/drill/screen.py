import os, sys, curses, signal
from curses.ascii import ESC
from logger import log

class ViewMode(object):
    def __init__(self, screen):
        self._screen = screen
        self._modechange = False
        log("switched to view mode")

    def process(self, ch):
        if self._modechange:
            if ch == ord(':'):
                log("changing mode")
                self._screen._mode = CommandMode(self._screen)
            else:
                log("not changing mode")
                self._modechange = False
        else:
            if ch == ESC:
                self._modechange = True
            elif ch == ord('q'):
                self._screen.quit()
            elif ch == ord('k'):
                self._screen.up()
            elif ch == ord('j'):
                self._screen.down()

class CommandMode(object):
    def __init__(self, screen):
        self._screen = screen
        log("switched to command mode")

    def process(self, ch):
        if ch == ord('q'):
            self._screen.quit()

class Screen(object):
    def __init__(self, screen):
        self._screen = screen
        self._lines = []
        self._top = 0
        # allocate the sub-pads
        h,w = screen.getmaxyx()
        log("screen is %i x %i" % (w, h))
        self._screensize = (h, w)
        self._inpad = curses.newpad(1, w)
        self._inwin = curses.newwin(w, 1, h - 1, 0)
        self._outpad = None
        self._outsize = (0, 0)
        self._mode = ViewMode(self)
        self._quit = False

    def append(self, lines):
        self._lines += [l.rstrip() for l in lines]
        self._refresh()
        log("appended %i lines" % len(lines))

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
            log("output pad is %i x %i" % (outputw, outputh))
            i = 0
            for l in self._lines:
                if l == '':
                    i += 1
                else:
                    try:
                        self._outpad.addstr(i, 0, l)
                    except:
                        log("failed to print string '%s': line %i, width is %i" % (l, i, len(l)))
                        raise
                    q,r = divmod(len(l), outputw)
                    i += q
                    if r > 0: i += 1
        # refresh the output window
        self._outpad.refresh(self._top, 0, 0, 0, screenh - 2, screenw - 2)

    def up(self):
        if self._top > 0:
            self._top -= 1
            self._refresh()

    def down(self):
        screenh,screenw = self._screensize
        outputh,outputw = self._outsize
        if self._top < outputh - screenh:
            self._top += 1
            self._refresh()

    def quit(self):
        self._quit = True

    def run(self, screen):
        while self._quit == False:
            self._refresh()
            ch = self._inwin.getch()
            self._mode.process(ch)
