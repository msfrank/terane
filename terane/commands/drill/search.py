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
from xmlrpclib import Fault
from csv import DictWriter
from twisted.web.xmlrpc import Proxy
from terane.commands.drill.ui import ui, useMainThread
from terane.loggers import getLogger

logger = getLogger('terane.commands.drill.driller')

class SearchResult(urwid.WidgetWrap):
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

class SearcherListbox(urwid.WidgetWrap):
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
        r = SearchResult(r)
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
        if key.startswith('command'):
            cmd = key.split()[1:]
            if cmd[0] == 'write':
                with file(cmd[1], 'wb') as f:
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
            return None
        return key       

class Searcher(urwid.WidgetWrap):
    def __init__(self, host, query):
        self._host = host
        self._query = query
        self._body = SearcherListbox()
        self._error = None
        self._frame = urwid.Frame(self._body)
        urwid.WidgetWrap.__init__(self, self._frame)
        # make the search request
        proxy = Proxy("http://%s/XMLRPC" % self._host, allowNone=True)
        deferred = proxy.callRemote('search', self._query)
        deferred.addCallback(self._getResult)
        deferred.addErrback(self._getError)

    @useMainThread
    def _getResult(self, results):
        self._meta = results.pop(0)
        for r in results:
            self._body.append(r)
        # redraw the listbox widget
        ui.redraw()

    @useMainThread
    def _getError(self, failure):
        try:
            raise failure.value
        except Fault, e:
            errtext = "search failed: %s (code %i)" % (e.faultString,e.faultCode)
            logger.debug(errtext)
            self._frame.set_body(urwid.Filler(urwid.Text(errtext, align='center')))
        except BaseException, e:
            errtext = "search failed: %s" % str(e)
            logger.debug(errtext)
            self._frame.set_body(urwid.Filler(urwid.Text(errtext, align='center')))
        ui.redraw()

    def command(self, cmd, args):
        pass
