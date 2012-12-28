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

import os, sys, re, urwid, datetime, dateutil.tz
from itertools import cycle, islice
from csv import DictWriter
from terane.commands.console import filters
from terane.commands.console.console import console
from terane.loggers import getLogger

logger = getLogger('terane.commands.console.results')

class Result(urwid.WidgetWrap):
    def __init__(self, evid, defaultfield, defaultvalue, fields):
        self.evid = evid
        self.ts = datetime.datetime.fromtimestamp(evid.ts, dateutil.tz.tzutc())
        self.offset = evid.offset
        self.defaultfield = defaultfield
        self.defaultvalue = defaultvalue
        self.fields = dict(fields)
        self.visible = True
        self.highlighted = False
        self._text = urwid.Text('', wrap='any')
        urwid.WidgetWrap.__init__(self, self._text)

    def reformat(self, resultslist):
        # if any filter fails, then don't display the result
        for f in resultslist.filters:
            if f(self.fields) == False:
                self.visible = False
                return
        self.visible = True
        # reset highlighting
        self.highlighted = False
        def _highlight(_text, text_color, highlight_color):
            _text = str(_text)
            if resultslist.pattern == None:
                return [(text_color, _text)]
            markup = []
            iterables = [
                [(text_color, t) for t in resultslist.pattern.split(_text)],
                [(highlight_color, t) for t in resultslist.pattern.findall(_text)]
                ]
            if len(iterables[1]) > 0:
                self.highlighted = True
            pending = len(iterables)
            nexts = cycle(iter(it).next for it in iterables)
            while pending:
                try:
                    for next in nexts:
                        attr,text = next()
                        if text != '':
                            markup.append((attr,text))
                except StopIteration:
                    pending -= 1
                    nexts = cycle(islice(nexts, pending))
            return markup
        # convert timestamp timezone if necessary
        if resultslist.tz:
            ts = self.ts.astimezone(resultslist.tz)
        else:
            ts = self.ts
        # we always display these fields
        text = [(console.palette['date'], "%s: " % ts.strftime("%d %b %Y %H:%M:%S %Z"))]
        text.extend(_highlight(self.defaultvalue, console.palette['text'], console.palette['highlight']))
        # if we are not collapsed, then show all fields
        if not resultslist.collapsed:
            fields = sorted([(k,v) for k,v in self.fields.items() if k not in resultslist.hidefields and v != ''])
            for (k,v) in fields:
                text.append('\n')
                text.append((console.palette['field-name'], "  %s=" % k))
                text.extend(_highlight(v, console.palette['field-value'], console.palette['highlight']))
        self._text.set_text(text)
        # return the results object itself, which makes this method useful for chaining
        return self

class ResultsListWalker(urwid.ListWalker):
    def __init__(self, maxsize=1000):
        self.maxsize = maxsize
        self.reset()

    def append(self, result):
        self.results.append(result)
        if len(self.results) > self.maxsize:
            self.results.pop(0)
        self._modified()

    def reset(self):
        self.results = []
        self.pos = 0
        self._modified()

    def resize(self, maxsize):
        self.maxsize = maxsize
        if maxsize > len(self.results):
            return
        remove = len(self.results) - maxsize
        for _ in range(remove):
            self.results.pop(0)
        self.pos = 0

    def __iter__(self):
        return iter(self.results)

    def __getitem__(self, i):
        return self.results[i]

    def __len__(self):
        return len(self.results)

    def get_focus(self):
        if len(self.results) == 0:
            focus = (None, None)
        else:
            focus = (self.results[self.pos], self.pos)
        return focus

    def set_focus(self, position):
        self.pos = position
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
                return (widget, position)
        return (None, None)

class ResultsListbox(urwid.WidgetWrap):
    def __init__(self):
        self._results = ResultsListWalker(console.scrollback)
        self._fields = []
        self.collapsed = True
        self.hidefields = []
        self.filters = []
        self.pattern = None
        self.tz = console.tz
        self._listbox = urwid.ListBox(self._results)
        urwid.WidgetWrap.__init__(self, self._listbox)
 
    def append(self, evid, defaultfield, defaultvalue, fields):
        result = Result(evid, defaultfield, defaultvalue, fields).reformat(self)
        self._results.append(result)
        self._fields += [f for f in result.fields.keys() if f not in self._fields]

    def keypress(self, size, key):
        if key == 'up' or key == 'k':
            self._listbox.keypress(size, 'up')
        if key == 'page up' or key == 'ctrl u':
            self._listbox.keypress(size, 'page up')
        if key == 'down' or key == 'j':
            self._listbox.keypress(size, 'down')
        if key == 'page down' or key == 'ctrl d':
            self._listbox.keypress(size, 'page down')
        if key == 'c':
            self.collapsed = not self.collapsed
            for r in self._results:
                r.reformat(self)

    def command(self, cmd, args):
        if cmd == 'filter':
            self.pushfilter(args.split())
        if cmd == 'pop':
            self.popfilter()
        if cmd == 'find':
            self.find(args)
        if cmd == 'rfind':
            self.rfind(args)
        if cmd == 'clear':
            self.clear()
        if cmd == 'save':
            self.save(args)

    def setvar(self, name, value):
        if name == 'timezone':
            tz = dateutil.tz.gettz(value)
            if tz != None:
                self.tz = tz
                self.redraw()
                logger.debug("set %s = %s" % (name, value))
            else:
                console.error("Unknown timezone '%s'" % value)
        if name == 'scrollback':
            try:
                self._results.resize(int(value))
                self.redraw()
                logger.debug("set %s = %s" % (name, value))
            except:
                console.error("Invalid scrollback value '%s'" % value)

    def redraw(self):
        logger.debug("redrawing ResultsListbox")
        for r in self._results:
            r.reformat(self)
        self._results._modified()

    def find(self, args):
        # if there is a new search regex, then redo results highlighting
        if args != '/':
            args = args[1:]
            logger.debug("highlighting results using regex '%s'" % args)
            self.pattern = re.compile(args)
            for r in self._results:
                r.reformat(self)
        # scroll to the position of next highlighted item
        widget,position = self._listbox.get_focus()
        nextposition = position + 1
        while nextposition < len(self._results):
            if self._results[nextposition].highlighted:
                break
            nextposition += 1
        # if the position remains the same, then just return
        if position == nextposition or nextposition >= len(self._results):
            return
        logger.debug("jumping to highlighted result at position %i" % nextposition)
        self._listbox.set_focus(nextposition, 'above')
        #self._listbox.set_focus_valign('bottom')
        #logger.debug("listbox offset=%i, inset=%i" % (self._listbox._offset,self._listbox._inset))

    def rfind(self, args):
        # if there is a new search regex, then redo results highlighting
        if args != '?':
            args = args[1:]
            logger.debug("highlighting results using regex '%s'" % args)
            self.pattern = re.compile(args)
            for r in self._results:
                r.reformat(self)
        # scroll to the position of next highlighted item
        widget,position = self._listbox.get_focus()
        prevposition = position - 1
        while prevposition >= 0:
            if self._results[prevposition].highlighted:
                break
            prevposition -= 1
        # if the position remains the same, then just return
        if position == prevposition or prevposition <= 0:
            return
        logger.debug("jumping to highlighted result at position %i" % prevposition)
        self._listbox.set_focus(prevposition, 'below')
        #self._listbox.set_focus_valign('top')
        #logger.debug("listbox offset=%i, inset=%i" % (self._listbox._offset,self._listbox._inset))

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
        logger.debug("pushing filter %s" % f)
        # run each result through the new filter chain
        self.filters.append(f)
        for r in self._results:
            r.reformat(self)
        self._results.reset_focus()
        self._results._modified()

    def popfilter(self):
        try:
            f = self.filters.pop(-1)
            logger.debug("popping filter %s" % f)
        except:
            return
        for r in self._results:
            r.reformat(self)
        self._results.reset_focus()
        self._results._modified()

    def clear(self):
        self._results.reset()

    def save(self, args):
        logger.debug("saving search to %s" % args)
        with file(args, 'w') as f:
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
