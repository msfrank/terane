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

import os, sys, dateutil.parser, datetime, xmlrpclib
from getpass import getpass
from logging import StreamHandler, DEBUG, Formatter
from twisted.web.xmlrpc import Proxy
from twisted.internet import reactor
from terane.settings import ConfigureError
from terane.loggers import startLogging, StdoutHandler, DEBUG

class Command(object):
    """
    Base command class which commands inherit from.  This class contains global
    settings parsing, and sets up the XMLRPC proxy.
    """

    def configure(self, settings):
        # load configuration
        section = settings.section("grok")
        self.host = section.getString("host", 'localhost:45565')
        self.username = section.getString("username", None)
        self.password = section.getString("password", None)
        if section.getBoolean("prompt password", False):
            self.password = getpass("Password: ")
        logconfigfile = section.getString('log config file', "%s.logconfig" % settings.appname)
        # configure server logging
        if section.getBoolean("debug", False):
            startLogging(StdoutHandler(), DEBUG, logconfigfile)
        else:
            startLogging(None)

    def _callRemote(self, method, *args, **kwds):
        proxy = Proxy("http://%s/XMLRPC" % self.host, user=self.username,
            password=self.password, allowNone=True)
        deferred = proxy.callRemote(method, *args, **kwds)
        deferred.addCallbacks(self.onResult, self.onError)
        deferred.addCallbacks(self._stopReactor, self._stopReactor)
        reactor.run()
        return 0

    def onResult(self, results):
        pass

    def onError(self, failure):
        try:
            raise failure.value
        except xmlrpclib.Fault, e:
            print "Command failed: %s (code %i)" % (e.faultString,e.faultCode)
        except ValueError, e:
            print "Command failed: remote server returned HTTP status %s: %s" % e.args
        except BaseException, e:
            print "Command failed: %s" % str(e)

    def _stopReactor(self, unused):
        reactor.stop()

class ListIndices(Command):

    def run(self):
        return self._callRemote("listIndices")

    def onResult(self, results):
        meta = results.pop(0)
        if len(results) > 0:
            for row in results: print "%s" % row
 
class ShowIndex(Command):

    def configure(self, settings):
        Command.configure(self, settings)
        args = settings.args()
        if len(args) != 1:
            raise ConfigureError("must specify an index")
        self.index = args[0]

    def run(self):
        return self._callRemote("showIndex", self.index)

    def onResult(self, results):
        meta = results.pop(0)
        for key,value in sorted(meta.items()):
            print "%s: %s" % (key,value)
        print "fields: %s" % ', '.join(results)

class ShowStats(Command):

    def configure(self, settings):
        Command.configure(self, settings)
        section = settings.section("show-stats")
        self.recursive = section.getBoolean("recursive", False)
        args = settings.args()
        if len(args) != 1:
            raise ConfigureError("must specify a statistic name")
        self.stat = args[0]

    def run(self):
        return self._callRemote("showStats", self.stat, self.recursive)

    def onResult(self, results):
        for key,value in results:
            print "%s: %s" % (key,value)

class FlushStats(Command):
    
    def configure(self, settings):
        Command.configure(self, settings)
        section = settings.section("flush-stats")
        self.flushAll = section.getBoolean("flush all", False)

    def run(self):
        return self._callRemote("showStats", self.stat, self.recursive)
