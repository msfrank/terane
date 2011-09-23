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
from csv import DictReader
from twisted.internet import reactor
from terane.commands.console.results import ResultsListbox
from terane.commands.console.ui import ui, useMainThread
from terane.loggers import getLogger

logger = getLogger('terane.commands.console.outfile')

class Outfile(urwid.WidgetWrap):
    def __init__(self, console, args):
        self._console = console
        self._path = os.path.expanduser(args)
        self.title = "Results from file %s" % self._path
        self._results = ResultsListbox()
        reactor.callLater(0, self._load)
        urwid.WidgetWrap.__init__(self, self._results)

    @useMainThread
    def _load(self):
        try:
            # load data from path
            with file(self._path, 'r') as f:
                logger.debug("opened outfile %s" % self._path)
                reader = DictReader(f)
                for row in reader:
                    self._results.append(row)
                ui.redraw()
        except BaseException, e:
            # close the search window
            #self._console.closeWindow(self)
            # display the error on screen
            errtext = "load failed: %s" % str(e)
            ui.error(errtext)
            logger.debug(errtext)

    def command(self, cmd, args):
        if self._results != None:
            return self._results.command(cmd, args)
        return None
