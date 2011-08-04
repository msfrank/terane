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

import os, sys, dateutil.parser, xmlrpclib
from logging import StreamHandler, DEBUG, Formatter
from twisted.web.xmlrpc import Proxy
from twisted.internet import reactor
from terane.loggers import startLogging, StdoutHandler, DEBUG

class Tailer(object):

    def configure(self, settings):
        # load configuration
        section = settings.section("tail")
        self.host = section.getString("host", 'localhost:45565')
        self.limit = section.getInt("limit", 100)
        self.longfmt = section.getBoolean("long format", False)
        self.indices = section.getList(str, "use indices", None)
        self.refresh = section.getInt("refresh", 3)
        # get the list of fields to display
        self.fields = section.getList(str, "display fields", None)
        if not self.fields == None:
            if not 'default' in self.fields: self.fields.append('default')
            if not 'ts' in self.fields: self.fields.append('ts')
        # concatenate the command args into the query string
        self.query = ' '.join(settings.args())
        # configure server logging
        if section.getBoolean("debug", False):
            startLogging(StdoutHandler(), DEBUG)
        else:
            startLogging(None)

    def run(self):
        self._proxy = Proxy("http://%s/XMLRPC" % self.host)
        # make the XMLRPC call
        self.tail(0)
        reactor.run()

    def tail(self, last):
        deferred = self._proxy.callRemote('tail', self.query, last)
        deferred.addCallback(self.printResult)
        deferred.addCallback(self.rescheduleTail)
        deferred.addErrback(self.printError)

    def printResult(self, results):
        meta = results.pop(0)
        if len(results) > 0:
            last = meta['last']
            for doc in results:
                ts = dateutil.parser.parse(doc['ts']).strftime("%d %b %Y %H:%M:%S")
                print "%s: %s" % (ts, doc['default'])
                if self.longfmt:
                    del doc['default']
                    del doc['ts']
                    for field,value in sorted(doc.items(), key=lambda x: x[0]):
                        if self.fields and field not in self.fields:
                            continue
                        print "\t%s=%s" % (field,value)
        return meta['last']

    def printError(self, failure):
        try:
            raise failure.value
        except xmlrpclib.Fault, e:
            print "Search failed: %s (code %i)" % (e.faultString,e.faultCode)
        except BaseException, e:
            print "Search failed: %s" % str(e)
        reactor.stop()

    def rescheduleTail(self, last):
        reactor.callLater(self.refresh, self.tail, last)
