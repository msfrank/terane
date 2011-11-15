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
from terane.commands.console.switcher import Window
from terane.commands.console.results import ResultsListbox
from terane.commands.console.console import console
from terane.commands.console.ui import useMainThread
from terane.loggers import getLogger

logger = getLogger('terane.commands.console.outfile')

class Outfile(Window):
    def __init__(self, args):
        self._path = os.path.expanduser(args)
        title = "Results from file %s" % self._path
        self._results = ResultsListbox()
        Window.__init__(self, title, self._results)

    def startService(self):
        logger.debug("startService")
        try:
            # load data from path
            with file(self._path, 'r') as f:
                logger.debug("opened outfile %s" % self._path)
                reader = DictReader(f)
                for row in reader:
                    self._results.append(row)
                console.redraw()
        except BaseException, e:
            # close the search window
            console.switcher.closeWindow(console.switcher.findWindow(self))
            # display the error on screen
            errtext = "Load failed: %s" % str(e)
            console.error(errtext)
            logger.debug(errtext)

    def stopService(self):
        logger.debug("stopService")

    def command(self, cmd, args):
        if self._results != None:
            return self._results.command(cmd, args)
        return None

    def setvar(self, name, value):
        self._results.setvar(name, value)

    def redraw(self):
        self._results.redraw()
