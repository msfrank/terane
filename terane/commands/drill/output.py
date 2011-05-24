import os, sys, curses, dateutil.parser
from terane.loggers import getLogger

logger = getLogger('terane.commands.drill.output')

class Event(object):
    def __init__(self, fields):
        default = fields['default']
        ts = dateutil.parser.parse(fields['ts']).strftime("%d %b %Y %H:%M:%S")
        self._line = "%s: %s" % (ts, default)
        self.size = (0, 0)

    def resize(self, screenw):
        if len(self._line) == 0:
            self.size = (1, screenw)
        else:
            q,r = divmod(len(self._line), screenw)
            if r > 0: q += 1
            self.size = (q, screenw)

    def draw(self, win, y, x):
        win.addstr(y, x, self._line)

class Output(object):
    def __init__(self, screen):
        self._screen = screen
        self._events = []
        self._pos = 0
        # allocate the sub-pads
        h,w = screen.getmaxyx()
        logger.debug("screen is %i x %i" % (w, h))
        self._screensize = (h, w)
        self._outpad = None
        self._outsize = (0, 0)
    
    def append(self, fields):
        ev = Event(fields)
        ev.resize(self._screensize[1])
        self._events.append(ev)
        self._refresh()
 
    def _refresh(self):
        screenh,screenw = self._screensize
        # calculate the height of the output
        lineh = 0
        for ev in self._events:
            lineh += ev.size[0]
        # if the screen width has changed, or the line
        # height exceeds the output window height, then
        # redraw the lines to the output window
        outputh,outputw = self._outsize
        if screenw != outputw or lineh > outputh:
            self._outpad = curses.newpad(lineh, screenw)
            self._outsize = (lineh, screenw)
            outputh,outputw = self._outsize
            logger.debug("allocated new output pad of size %i x %i" % (outputw, outputh))
            i = 0
            for ev in self._events:
                ev.draw(self._outpad, i, 0)
                i += ev.size[0]
        # refresh the output window
        self._outpad.refresh(self._pos, 0, 0, 0, screenh - 2, screenw - 2)

    def scrollUp(self):
        if self._pos > 0:
            self._pos -= 1
            self._refresh()

    def scrollDown(self):
        screenh,screenw = self._screensize
        outputh,outputw = self._outsize
        if self._pos < outputh - screenh:
            self._pos += 1
            self._refresh()

    def toggleCollapsed(self):
        pass
