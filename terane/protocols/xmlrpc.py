#Copyright 2010,2011 Michael Frank <msfrank@syntaxjockey.com>
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

import xmlrpclib, functools
from twisted.internet import threads
from twisted.web.xmlrpc import XMLRPC
from twisted.web.server import Site
from twisted.application.service import Service
from zope.interface import implements
from terane.plugins import Plugin, IPlugin
from terane.query import queries
from terane.query.dql import QuerySyntaxError
from terane.stats import stats
from terane.loggers import getLogger

logger = getLogger('terane.protocols.xmlrpc')

searches = stats.get('terane.protocols.xmlrpc.searches', 0, int)
tails = stats.get('terane.protocols.xmlrpc.tails', 0, int)
totalsearchtime = stats.get('terane.protocols.xmlrpc.search.totaltime', 0.0, float)
totaltailtime = stats.get('terane.protocols.xmlrpc.tail.totaltime', 0.0, float)

class FaultInternalError(xmlrpclib.Fault):
    def __init__(self):
        xmlrpclib.Fault.__init__(self, 1001, "Internal Error")

class FaultBadRequest(xmlrpclib.Fault):
    def __init__(self, info):
        xmlrpclib.Fault.__init__(self, 1002, "Bad Request: %s" % str(info))

def useThread(fn):
    """
    A decorator for methods which should be run in a separate thread.
    """
    @functools.wraps(fn)
    def _threadWrapper(*args, **kwds):
        return threads.deferToThread(fn, *args, **kwds)
    return _threadWrapper

class XMLRPCDispatcher(XMLRPC):

    def __init__(self):
        XMLRPC.__init__(self)

    @useThread
    def xmlrpc_search(self, query, indices=None, limit=100, reverse=False, fields=None):
        try:
            searches.value += 1
            results = queries.search(unicode(query), indices, limit, None, ("ts",), reverse, fields)
            totalsearchtime.value += float(results[0]['runtime'])
            return list(results)
        except QuerySyntaxError, e:
            raise FaultBadRequest(e)
        except BaseException, e:
            logger.exception(e)
            raise FaultInternalError()

    @useThread
    def xmlrpc_tail(self, query, last, indices=None, limit=100, fields=None):
        try:
            tails.value += 1
            results = queries.tail(unicode(query), last, indices, limit, fields)
            totaltailtime.value += float(results[0]['runtime'])
            return list(results)
        except QuerySyntaxError, e:
            raise FaultBadRequest(e)
        except BaseException, e:
            logger.exception(e)
            raise FaultInternalError()

    @useThread
    def xmlrpc_listIndices(self):
        try:
            return list(queries.listIndices())
        except BaseException, e:
            logger.exception(e)
            raise FaultInternalError()

    @useThread
    def xmlrpc_showIndex(self, name):
        try:
            return list(queries.showIndex(name))
        except BaseException, e:
            logger.exception(e)
            raise FaultInternalError()

class XMLRPCProtocolPlugin(Plugin):

    implements(IPlugin)

    def __init__(self):
        Plugin.__init__(self)
        self._instance = None

    def configure(self, section):
        self.listenAddress = section.getString('listen address', '0.0.0.0')
        self.listenPort = section.getInt('listen port', 45565)

    def startService(self):
        Plugin.startService(self)
        from twisted.internet import reactor
        self.listener = reactor.listenTCP(self.listenPort, Site(XMLRPCDispatcher()), interface=self.listenAddress)
        logger.debug("[%s] started xmlrpc listener on %s:%i" % (self.name, self.listenAddress, self.listenPort))

    def stopService(self):
        Plugin.stopService(self)
        self.listener.stopListening()
        logger.debug("[%s] stopped xmlrpc listener" % self.name)
