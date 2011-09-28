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
from terane.commands.console.ui import ui
from terane.loggers import getLogger

logger = getLogger('terane.commands.console.switcher')

class WindowHandle(object):
    def __init__(self, window, title, wid):
        self.window = window
        self.title = title
        self.wid = wid
        self.prev = None
        self.next = None

    def getText(self, isFocused=False, isCurr=False):
        if isFocused == True:
            attr = 'highlight'
        else:
            attr = 'normal'
        if isCurr == True:
            text = "* %s" % self.title
        else:
            text = self.title
        return urwid.Text((attr, text))

class WindowSwitcher(urwid.WidgetWrap, urwid.ListWalker):
    def __init__(self, frame):
        self._frame = frame
        self._blank = urwid.SolidFill()
        self._windows = None
        self._nwindows = 0
        self._curr = None
        self._nextwid = 1
        self._focus = None
        self._windowlist = urwid.ListBox(self)
        urwid.WidgetWrap.__init__(self, self._windowlist)

    def keypress(self, size, key):
        if self._frame.get_body() == self:
            if key == 'up' or key == 'k':
                logger.debug("window switcher up")
                self._windowlist.keypress(size, 'up')
                self._windowlist.set_focus(self._focus - 1, 'below')
            if key == 'down' or key == 'j':
                logger.debug("window switcher down")
                self._windowlist.keypress(size, 'down')
                self._windowlist.set_focus(self._focus + 1, 'above')
            if key == 'enter':
                logger.debug("window switcher: jump to '%s'" % self[self._focus].title)
            return None
        if self._curr != None:
            return self._curr.window.keypress(size, key)
        return key

    def command(self, cmd, args):
        # window management commands
        if cmd == 'windows':
            return self.showWindowlist()
        if cmd == 'prev':
            return self.prevWindow()
        if cmd == 'next':
            return self.nextWindow()
        if cmd == 'jump':
            try:
                return self.jumpToWindow(self.findWindow(int(args)))
            except IndexError:
                ui.error(Exception("No such window '%s'" % args))
            except BaseException, e:
                ui.error(e)
            return None
        if cmd == 'close':
            if args == '':
                return self.closeWindow(self._curr)
            else:
                try:
                    return self.closeWindow(self.findWindow(int(args)))
                except IndexError:
                    ui.error(Exception("No such window '%s'" % args))
                except BaseException, e:
                    ui.error(e)
            return None
        # forward other commands to the active window
        if self._curr != None:
            return self._curr.window.command(cmd, args)
        return None

    def showWindowlist(self):
        window = self._frame.get_body()
        # window list is already displayed
        if window == self:
            return
        if self._windows == None:
            self._focus = None
        else:
            self._focus = 0
        self._frame.set_body(self)
        self._frame.set_focus('footer')

    def _hideWindowList(self):
        window = self._frame.get_body()
        # window list is not currently displayed
        if window != self:
            return
        if self._curr == None:
            self._frame.set_body(self._blank)
        else:
            self._frame.set_body(self._curr.window)
        self._frame.set_focus('footer')

    def findWindow(self, target):
        if self._windows != None:
            curr = self._windows
            while True:
                if isinstance(target, int) and curr.id == int(target):
                    return curr
                if isinstance(target, str) and re.search(target, curr.title) != None:
                    return curr
                if curr.window == target:
                    return curr
                curr = curr.next
                if curr == self._windows:
                    break
        raise IndexError

    def addWindow(self, window):
        """
        Add the specified window to the window list and bring it to the front.
        """
        try:
            self.findWindow(window)
            raise Exception("Failed to add window: window is already added")    
        except:
            pass
        try:
            title = "Window #%i - %s" % (self._nextwid, getattr(window, 'title'))
        except:
            title = "Window #%i" % self._nextwid
        handle = WindowHandle(window, title, self._nextwid)
        if self._nwindows == 0:
            self._windows = handle
            self._curr = handle
            self._curr.prev = handle
            self._curr.next = handle
        elif self._nwindows == 1:
            handle.prev = self._windows
            handle.next = self._windows
            self._windows.prev = handle
            self._windows.next = handle
            self._curr = handle
        else:
            handle.prev = self._curr
            handle.next = self._curr.next
            self._curr.next = handle
            self._curr.prev.next = handle
            self._curr = handle
        self._nwindows += 1
        self._nextwid += 1
        self._frame.set_body(window)
        self._frame.set_focus('footer')

    def nextWindow(self):
        """
        Cycle to the next window in the window list.
        """
        window = self._frame.get_body()
        if window == self:
            return self._hideWindowList()
        if self._windows == None:
            return
        self._curr = self._curr.next
        self._frame.set_body(self._curr.window)
        self._frame.set_focus('footer')

    def prevWindow(self):
        """
        Cycle to the previous window in the window list.
        """
        window = self._frame.get_body()
        if window == self:
            return self._hideWindowList()
        if self._windows == None:
            return
        self._curr = self._curr.prev
        self._frame.set_body(self._curr.window)
        self._frame.set_focus('footer')

    def jumpToWindow(self, handle):
        self._curr = handle
        self._frame.set_body(self._curr.window)
        self._frame.set_focus('footer')

    def closeWindow(self, handle):
        # detach handle from the circular linked list
        handle.prev.next = handle.next
        handle.next.prev = handle.prev
        self._nwindows -= 1
        # fix up the _windows reference, if needed
        if self._windows == handle:
            if self._nwindows == 0:
                self._windows = None
            else:
                self._windows = handle.next
        # fix up the _curr reference, if needed
        if self._curr == handle:
            if self._nwindows == 0:
                self._curr = None
            else:
                # display the new current window   
                self._curr = handle.next
                self._frame.set_body(self._curr.window)
                self._frame.set_focus('footer')

    def __len__(self):
        return self._nwindows

    def __getitem__(self, index):
        curr = self._windows
        if curr == None:
            raise IndexError()
        i = index
        if i >= 0:
            while i > 0:
                curr = curr.next
                i -= 1
        else:
            while i < 0:
                curr = curr.prev
                i += 1
        #logger.debug("__getitem__: index=%i, title='%s'" % (index,curr.title))
        return curr

    def get_focus(self):
        if self._focus == None:
            return (None,None)
        handle = self[self._focus]
        isCurr = False
        if self._curr == handle:
            isCurr = True
        #logger.debug("get_focus: focus=%i, title='%s'" % (self._focus,handle.title))
        return (handle.getText(isFocused=True, isCurr=isCurr), self._focus)

    def set_focus(self, focus):
        if focus < 0 or focus >= self._nwindows - 1:
            return
        self._focus = focus
        logger.debug("set_focus: focus=%i" % self._focus)
        urwid.ListWalker._modified(self)

    def get_prev(self, position):
        if position == None or position < 1:
            return (None,None)
        position = position - 1
        isCurr = False
        if self._curr == self[self._focus]:
            isCurr = True
        handle = self[position]
        logger.debug("get_prev: position=%i, title='%s'" % (position, handle.title))
        return (handle.getText(isCurr=isCurr), position)

    def get_next(self, position):
        if position == None or position >= self._nwindows - 1:
            return (None,None)
        position = position + 1
        isCurr = False
        if self._curr == self[self._focus]:
            isCurr = True
        handle = self[position]
        logger.debug("get_next: position=%i, title='%s'" % (position, handle.title))
        return (handle.getText(isCurr=isCurr), position)
