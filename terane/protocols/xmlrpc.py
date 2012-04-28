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
from terane.queries import queries, QueryExecutionError
from terane.bier.ql import QuerySyntaxError
from terane.loggers import getLogger
from terane.stats import getStat, stats

logger = getLogger('terane.protocols.xmlrpc')


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
        self.iters = getStat('terane.protocols.xmlrpc.iter.count', 0, int)
        self.tails = getStat('terane.protocols.xmlrpc.tail.count', 0, int)
        self.totalitertime = getStat('terane.protocols.xmlrpc.iter.totaltime', 0.0, float)
        self.totaltailtime = getStat('terane.protocols.xmlrpc.tail.totaltime', 0.0, float)

    @useThread
    def xmlrpc_iter(self, query, last=None, indices=None, limit=100, reverse=False, fields=None):
        try:
            self.iters += 1
            results,meta = queries.iter(unicode(query), last, indices, limit, reverse, fields)
            self.totalitertime += float(meta['runtime'])
            return [meta] + list(results)
        except (QuerySyntaxError, QueryExecutionError), e:
            raise FaultBadRequest(e)
        except BaseException, e:
            logger.exception(e)
            raise FaultInternalError()

    @useThread
    def xmlrpc_tail(self, query, last=None, indices=None, limit=100, fields=None):
        try:
            self.tails += 1
            results,meta = queries.tail(unicode(query), last, indices, limit, fields)
            self.totaltailtime += float(meta['runtime'])
            return [meta] + list(results)
        except (QuerySyntaxError, QueryExecutionError), e:
            raise FaultBadRequest(e)
        except BaseException, e:
            logger.exception(e)
            raise FaultInternalError()

    @useThread
    def xmlrpc_listIndices(self):
        try:
            indices,meta = queries.listIndices()
            return [meta] + list(indices)
        except (QuerySyntaxError, QueryExecutionError), e:
            raise FaultBadRequest(e)
        except BaseException, e:
            logger.exception(e)
            raise FaultInternalError()

    @useThread
    def xmlrpc_showIndex(self, name):
        try:
            fields,stats = queries.showIndex(name)
            return [stats] + fields
        except (QuerySyntaxError, QueryExecutionError), e:
            raise FaultBadRequest(e)
        except BaseException, e:
            logger.exception(e)
            raise FaultInternalError()

    @useThread
    def xmlrpc_showStats(self, name, recursive=False):
        try:
            return stats.showStats(name, recursive)
        except BaseException, e:
            logger.exception(e)
            raise FaultInternalError()

    @useThread
    def xmlrpc_flushStats(self, flushAll=False):
        try:
            stats.flushStats(flushAll)
            return []
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
