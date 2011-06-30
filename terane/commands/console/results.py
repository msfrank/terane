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
from dateutil.parser import parse
from csv import DictWriter
from terane.commands.console import filters
from terane.loggers import getLogger

logger = getLogger('terane.commands.console.results')

class Result(urwid.WidgetWrap):
    def __init__(self, fields):
        self.fields = fields
        self.visible = True
        self._text = urwid.Text('', wrap='any')
        urwid.WidgetWrap.__init__(self, self._text)

    def reformat(self, resultslist):
            # if any filter fails, then don't display the result
            for f in resultslist.filters:
                if f(self.fields) == False:
                    self.visible = False
                    return
            self.visible = True
            # we always display these fields
            default = self.fields['default']
            ts = parse(self.fields['ts']).strftime("%d %b %Y %H:%M:%S")
            # if we are collapsed, only show ts and default
            if resultslist.collapsed:
                self._text.set_text("%s: %s" % (ts, default))
            # otherwise show all fields
            else:
                fields = self.fields.copy()
                del fields['default']
                del fields['ts']
                fields = sorted(["  %s=%s" % (k,v) for k,v in fields.items() if k not in resultslist.hidefields])
                text = "%s: %s\n" % (ts,default) + "\n".join(fields) + "\n"
                self._text.set_text(text)

class ResultsListWalker(urwid.ListWalker):
    def __init__(self):
        self.results = []
        self.pos = 0

    def append(self, result):
        self.results.append(result)
        self._modified()

    def __iter__(self):
        return iter(self.results)

    def get_focus(self):
        if len(self.results) == 0:
            focus = (None, None)
        else:
            focus = (self.results[self.pos], self.pos)
        logger.debug("get_focus: %s (pos %i)" % focus)
        return focus

    def set_focus(self, position):
        self.pos = position
        logger.debug("set_focus: %s (pos %i)" % (self.results[self.pos], self.pos))
        self._modified()

    def reset_focus(self):
        widget,position = self.get_next(0)
        if not position == None:
            self.set_focus(position)

    def get_next(self, position):
        try:
            if position == None:
                return (None, None)
            while True:
                position += 1
                widget = self.results[position]
                if widget.visible:
                    break
            logger.debug("get_next: %s (pos %i)" % (widget,position))
            return (widget, position)
        except IndexError:
            return (None, None)

    def get_prev(self, position):
        if position == None:
            return (None, None)
        while position > 0:
            position -= 1
            widget = self.results[position]
            if widget.visible:
                logger.debug("get_prev: %s (pos %i)" % (widget,position))
                return (widget, position)
        logger.debug("get_prev: None (pos None)")
        return (None, None)

class ResultsListbox(urwid.WidgetWrap):
    def __init__(self):
        self._results = ResultsListWalker()
        self._fields = []
        self.collapsed = True
        self.hidefields = []
        self.filters = []
        # build the listbox widget
        self._listbox = urwid.ListBox(self._results)
        urwid.WidgetWrap.__init__(self, self._listbox)
 
    def append(self, r):
        r = Result(r)
        r.reformat(self)
        self._results.append(r)
        self._fields += [f for f in r.fields.keys() if f not in self._fields]

    def keypress(self, size, key):
        if key == 'up' or key == 'k':
            self._listbox.keypress(size, 'up')
            return None
        if key == 'page up' or key == 'ctrl u':
            self._listbox.keypress(size, 'page up')
            return None
        if key == 'down' or key == 'j':
            self._listbox.keypress(size, 'down')
            return None
        if key == 'page down' or key == 'ctrl d':
            self._listbox.keypress(size, 'page down')
            return None
        if key == 'c':
            self.collapsed = not self.collapsed
            for r in self._results:
                r.reformat(self)
            return None

    def command(self, cmd, args):
        if cmd == 'save':
            logger.debug("saving search to %s" % args[0])
            with file(args[0], 'w') as f:
                self.savecsv(f)
        if cmd == 'filter':
            logger.debug("pushing filter: %s" % ' '.join(args))
            self.pushfilter(args)
        if cmd == 'pop':
            logger.debug("popping filter")
            self.popfilter()

    def pushfilter(self, args):
        # parse filter arguments
        if len(args) < 3:
            return
        field = args[0]
        filtertype = args[1]
        if filtertype == '*': ftype = None
        params = args[2:]
        # create the new filter
        if filtertype == 'is':
            f = filters.Is(field, params)
        elif filtertype == 'contains':
            f = filters.Contains(field, params)
        elif filtertype == 'matches':
            f = filters.Matches(field, params)
        elif filtertype == 'gt':
            f = filters.GreaterThan(field, params)
        elif filtertype == 'lt':
            f = filters.LessThan(field, params)
        else:
            return
        # run each result through the new filter chain
        self.filters.append(f)
        for r in self._results:
            r.reformat(self)
        self._results.reset_focus()
        self._results._modified()

    def popfilter(self):
        try:
            self.filters.pop(-1)
        except:
            return
        for r in self._results:
            r.reformat(self)
        self._results.reset_focus()
        self._results._modified()

    def savecsv(self, f):
        # set the field order
        specialfields = ['ts','input','hostname','id','default']
        fields = [field for field in sorted(self._fields) if field not in specialfields]
        fields = specialfields + fields
        writer = DictWriter(f, fields)
        # write the header row
        writer.writerow(dict([(fname,fname) for fname in fields]))
        # write each result row
        for r in self._results:
            writer.writerow(r.fields)
