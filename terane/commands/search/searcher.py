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

class Searcher(object):
    def configure(self, settings):
        # load configuration
        section = settings.section("search")
        self.host = section.getString("host", 'localhost:45565')
        self.limit = section.getInt("limit", 100)
        self.reverse = section.getBoolean("display reverse", False)
        self.longfmt = section.getBoolean("long format", False)
        self.indices = section.getList(str, "use indices", None)
        # get the list of fields to display
        self.fields = section.getList(str, "display fields", None)
        if not self.fields == None:
            if not 'default' in self.fields: self.fields.append('default')
            if not 'ts' in self.fields: self.fields.append('ts')
        # concatenate the command args into the query string
        self.query = ' '.join(settings.args())
        # configure server logging
        logconfigfile = section.getString('log config file', "%s.logconfig" % settings.appname)
        if section.getBoolean("debug", False):
            startLogging(StdoutHandler(), DEBUG, logconfigfile)
        else:
            startLogging(None)

    def run(self):
        proxy = Proxy("http://%s/XMLRPC" % self.host, allowNone=True)
        deferred = proxy.callRemote('search', self.query, self.indices,
            self.limit, self.reverse, self.fields)
        deferred.addCallback(self.printResult)
        deferred.addErrback(self.printError)
        reactor.run()
        return 0

    def printResult(self, results):
        meta = results.pop(0)
        if len(results) > 0:
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
            print ""
            print "found %i matches in %f seconds." % (len(results), meta['runtime'])
        else:
            print "no matches found."
        reactor.stop()
 
    def printError(self, failure):
        try:
            raise failure.value
        except xmlrpclib.Fault, e:
            print "Search failed: %s (code %i)" % (e.faultString,e.faultCode)
        except BaseException, e:
            print "Search failed: %s" % str(e)
        reactor.stop()
