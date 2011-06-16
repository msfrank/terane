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
from terane.loggers import getLogger

logger = getLogger('terane.commands.console.results')

class Result(urwid.WidgetWrap):
    def __init__(self, fields):
        self.fields = fields
        self._text = urwid.Text('', wrap='any')
        urwid.WidgetWrap.__init__(self, self._text)

    def format_text(self, collapsed=True, hidefields=[]):
            default = self.fields['default']
            ts = parse(self.fields['ts']).strftime("%d %b %Y %H:%M:%S")
            if collapsed:
                self._text.set_text("%s: %s" % (ts, default))
            else:
                fields = self.fields.copy()
                del fields['default']
                del fields['ts']
                fields = sorted(["  %s=%s" % (k,v) for k,v in fields.items() if k not in hidefields])
                text = "%s: %s\n" % (ts,default) + "\n".join(fields) + "\n"
                self._text.set_text(text)

class ResultsListbox(urwid.WidgetWrap):
    def __init__(self):
        self._results = urwid.SimpleListWalker([])
        self._fields = []
        self._collapsed = True
        self._hidefields = []
        self._filters = []
        # build the listbox widget
        self._listbox = urwid.ListBox(self._results)
        urwid.WidgetWrap.__init__(self, self._listbox)
 
    def append(self, r):
        r = Result(r)
        r.format_text(self._collapsed, self._hidefields)
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
            self._collapsed = not self._collapsed
            for r in self._results:
                r.format_text(self._collapsed, self._hidefields)
            return None

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
