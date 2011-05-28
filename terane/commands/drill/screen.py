import os, sys, curses, signal
from twisted.internet import reactor
from terane.commands.drill.input import Input, ResizeScreen, GetchError
from terane.loggers import getLogger

logger = getLogger('terane.commands.drill.screen')

class Screen(object):
    def __init__(self, stdscr):
        self._stdscr = stdscr
        self._windows = []
        # determine initial screen dimensions
        self.height,self.width = h,w = stdscr.getmaxyx()
        logger.debug("screen is %i x %i" % (self.width,self.height))
        # handle input
        reactor.addReader(self)
        self._input = Input()
        # do the initial refresh
        self.refresh()
    
    def fileno(self):
        return 0

    def doRead(self):
        try:
            logger.debug("processing key...")
            command = self._input.process()
            if command != None:
                logger.debug("handling command %s" % command)
            else:
                logger.debug("processed key")
        except ResizeScreen:
            # determine the new screen dimensions
            self.height,self.width = self._stdscr.getmaxyx()
            logger.debug("resized screen to %i x %i" % (self.width,self.height))
            # refresh the screen
            self.refresh()
        except GetchError:
            logger.debug("getch returned error")

    def connectionLost(self, reason):
        pass

    def logPrefix(self):
        return "terane.commands.drill.screen"

    def refresh(self):
        if len(self._windows) > 0:
            curr = self._windows[0]
            curr.refresh(0, 0, self.height - 1, self.width)
        self._input.refresh(self.height - 1, 0, 1, self.width)

    def setWindow(self, window):
        self._windows = [window,]
