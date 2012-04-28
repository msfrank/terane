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
from logging import StreamHandler, DEBUG, Formatter
from twisted.web.xmlrpc import Proxy
from twisted.internet import reactor
from terane.settings import ConfigureError
from terane.loggers import startLogging, StdoutHandler, DEBUG

class Grokker(object):
    def __init__(self):
        self.methods = {
            'list-indices': ('listIndices', self.listIndicesResult),
            'show-index': ('showIndex', self.showIndexResult),
            'explain-query': ('explainQuery', self.explainQueryResult),
            'show-stats': ('showStats', self.showStatsResult),
            'flush-stats': ('flushStats', self.flushStatsResult),
            }

    def configure(self, settings):
        # load configuration
        section = settings.section("grok")
        self.host = section.getString("host", 'localhost:45565')
        logconfigfile = section.getString('log config file', "%s.logconfig" % settings.appname)
        # configure server logging
        if section.getBoolean("debug", False):
            startLogging(StdoutHandler(), DEBUG, logconfigfile)
        else:
            startLogging(None)
        try:
            self.methodname,self.resultfn = self.methods[settings.args()[0]]
        except IndexError:
            raise ConfigureError("missing required command argument")
        except KeyError, e:
            raise ConfigureError("unknown command '%s'" % str(e))
        self.args = settings.args()[1:]

    def run(self):
        proxy = Proxy("http://%s/XMLRPC" % self.host, allowNone=True)
        deferred = proxy.callRemote(self.methodname, *self.args)
        deferred.addCallback(self.resultfn)
        deferred.addErrback(self.printError)
        reactor.run()
        return 0

    def listIndicesResult(self, results):
        meta = results.pop(0)
        if len(results) > 0:
            for row in results: print "%s" % row
        reactor.stop()
 
    def showIndexResult(self, results):
        meta = results.pop(0)
        for key,value in sorted(meta.items()):
            print "%s: %s" % (key,value)
        print "fields: %s" % ', '.join(results)
        reactor.stop()

    def showStatsResult(self, results):
        for key,value in results:
            print "%s: %s" % (key,value)
        reactor.stop()

    def flushStatsResult(self, results):
        reactor.stop()

    def explainQueryResult(self, results):
        pass

    def printError(self, failure):
        try:
            raise failure.value
        except xmlrpclib.Fault, e:
            print "Command failed: %s (code %i)" % (e.faultString,e.faultCode)
        except BaseException, e:
            print "Command failed: %s" % str(e)
        reactor.stop()
