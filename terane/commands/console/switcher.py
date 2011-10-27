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
from twisted.application.service import Service, MultiService
from terane.loggers import getLogger

logger = getLogger('terane.commands.console.switcher')

class Window(Service, urwid.WidgetWrap):
    def __init__(self, title, body):
        Service.__init__(self)
        self.body = body
        self.title = title
        self.wid = None
        self.prev = None
        self.next = None
        urwid.WidgetWrap.__init__(self, body)

class WindowSwitcher(MultiService, urwid.WidgetWrap, urwid.ListWalker):
    def __init__(self, frame):
        MultiService.__init__(self)
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
                self._windowlist.keypress(size, 'up')
                self._windowlist.set_focus(self._focus - 1, 'below')
            if key == 'down' or key == 'j':
                self._windowlist.keypress(size, 'down')
                self._windowlist.set_focus(self._focus + 1, 'above')
            if key == 'enter':
                self.jumpToWindow(self[self._focus])
            return None
        if self._curr != None:
            return self._curr.window.keypress(size, key)
        return key

    def command(self, cmd, args):
        from terane.commands.console.console import console
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
                console.error(Exception("No such window '%s'" % args))
            except BaseException, e:
                console.error(e)
            return None
        if cmd == 'close':
            if args == '':
                window = self._frame.get_body()
                if window == self:
                    return self._hideWindowList()
                return self.closeWindow(self._curr)
            else:
                try:
                    return self.closeWindow(self.findWindow(int(args)))
                except IndexError:
                    console.error(Exception("No such window '%s'" % args))
                except BaseException, e:
                    console.error(e)
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
        title = "Window #%i - %s" % (self._nextwid, window.title)
        window.wid = self._nextwid
        if self._nwindows == 0:
            self._windows = window
            window.prev = window
            window.next = window
            self._curr = window
        elif self._nwindows == 1:
            self._windows.prev = window
            self._windows.next = window
            window.prev = self._windows
            window.next = self._windows
            self._curr = window
        else:
            window.prev = self._windows.prev
            window.next = self._windows
            self._windows.prev.next = window
            self._windows.prev = window
            self._curr = window
        self._nwindows += 1
        self._nextwid += 1
        self._frame.set_body(window)
        self._frame.set_focus('footer')
        self.addService(window)

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

    def closeWindow(self, window):
        # detach window from the circular linked list
        window.prev.next = window.next
        window.next.prev = window.prev
        self._nwindows -= 1
        # fix up the _windows reference, if needed
        if self._windows == window:
            if self._nwindows == 0:
                self._windows = None
            else:
                self._windows = window.next
        # fix up the _curr reference, if needed
        if self._curr == window:
            if self._nwindows == 0:
                self._curr = None
            else:
                # display the new current window   
                self._curr = window.next
                self._frame.set_body(self._curr.window)
                self._frame.set_focus('footer')
        self.removeService(window)

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
        return curr

    def _getText(self, window, isFocused=False, isCurr=False):
        if isFocused == True:
            attr = 'highlight'
        else:
            attr = 'normal'
        if isCurr == True:
            text = "* %s" % window.title
        else:
            text = window.title
        return urwid.Text((attr, text))

    def get_focus(self):
        if self._focus == None:
            return (None,None)
        handle = self[self._focus]
        isCurr = False
        if self._curr == handle:
            isCurr = True
        return (self._getText(handle, True, isCurr), self._focus)

    def set_focus(self, focus):
        if focus < 0 or focus > self._nwindows - 1:
            return
        self._focus = focus
        urwid.ListWalker._modified(self)

    def get_prev(self, position):
        if position == None or position < 1:
            return (None,None)
        position = position - 1
        handle = self[position]
        isCurr = False
        if self._curr == handle:
            isCurr = True
        return (self._getText(handle, isCurr=isCurr), position)

    def get_next(self, position):
        if position == None or position >= self._nwindows - 1:
            return (None,None)
        position = position + 1
        handle = self[position]
        isCurr = False
        if self._curr == handle:
            isCurr = True
        return (self._getText(handle, isCurr=isCurr), position)
