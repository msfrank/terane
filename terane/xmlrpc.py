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

import xmlrpclib, traceback, functools
from twisted.internet import threads
from twisted.web.xmlrpc import XMLRPC
from twisted.web.server import Site
from terane.plugins import Plugin
from terane.dql import parseQuery, ParsingSyntaxError
from terane.db.plan import SearchPlan, TailPlan, ListIndicesPlan, ShowIndexPlan
from terane.stats import stats
from terane.loggers import getLogger

MAX_ID = 2**64

logger = getLogger('terane.xmlrpc')

queries = stats.get('terane.xmlrpc.queries', 0, int)
tails = stats.get('terane.xmlrpc.tails', 0, int)
totalsearchtime = stats.get('terane.xmlrpc.search.totaltime', 0.0, float)
totaltailtime = stats.get('terane.xmlrpc.tail.totaltime', 0.0, float)

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

    def logexception(self, e):
        logger.debug("XMLRPC method exception: %s\n--------\n%s--------" % (e, traceback.format_exc()))

    @useThread
    def xmlrpc_search(self, query, indices=None, limit=100, reverse=False, fields=None):
        try:
            queries.value += 1
            # create a new execution plan
            plan = SearchPlan(parseQuery(unicode(query)), indices, limit, None, ("ts",), reverse, fields)
            # execute the plan
            results = plan.execute()
            # add to search query time counter
            totalsearchtime.value += float(results[0]['runtime'])
            # return RPC result
            return list(results)
        except ParsingSyntaxError, e:
            raise FaultBadRequest(e)
        except BaseException, e:
            self.logexception(e)
            raise FaultInternalError()

    @useThread
    def xmlrpc_tail(self, query, last, indices=None, limit=100, fields=None):
        try:
            tails.value += 1
            # create a new execution plan
            plan = TailPlan(parseQuery(unicode(query)), last, indices, limit, fields)
            # execute the plan
            results = plan.execute()
            # add to the tail query time counter
            totaltailtime.value += float(results[0]['runtime'])
            # return RPC result
            return list(results)
        except ParsingSyntaxError, e:
            raise FaultBadRequest(e)
        except BaseException, e:
            self.logexception(e)
            raise FaultInternalError()

    @useThread
    def xmlrpc_listIndices(self):
        try:
            plan = ListIndicesPlan()
            # return RPC result
            return list(plan.execute())
        except BaseException, e:
            self.logexception(e)
            raise FaultInternalError()

    @useThread
    def xmlrpc_showIndex(self, name):
        try:
            plan = ShowIndexPlan(name)
            # return RPC result
            return list(plan.execute())
        except BaseException, e:
            self.logexception(e)
            raise FaultInternalError()

class XMLRPCService(Plugin):

    def configure(self, section):
        pass

    def startService(self):
        Plugin.startService(self)
        from twisted.internet import reactor
        self.listener = reactor.listenTCP(7080, Site(XMLRPCDispatcher()))
        logger.debug("[xmlrpc] started xmlrpc service")

    def stopService(self):
        self.listener.stopListening()
        self.listener = None
        logger.debug("[xmlrpc] stopped xmlrpc service")
        Plugin.stopService(self)
