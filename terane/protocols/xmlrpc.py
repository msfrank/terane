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

import xmlrpclib
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web.xmlrpc import XMLRPC
from twisted.web.server import Site
from twisted.cred.portal import IRealm
from twisted.web.resource import IResource
from twisted.web.guard import HTTPAuthSessionWrapper, BasicCredentialFactory
from twisted.application.service import Service
from zope.interface import implements
from terane import IManager
from terane.plugins import Plugin, IPlugin
from terane.protocols import IProtocol, Protocol
from terane.registry import getRegistry
from terane.queries import QueryExecutionError
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

class FaultNotAuthorized(xmlrpclib.Fault):
    def __init__(self, info):
        xmlrpclib.Fault.__init__(self, 1003, "Not Authorized: %s" % str(info))

class XMLRPCDispatcher(XMLRPC):

    def __init__(self, avatarId):
        XMLRPC.__init__(self, allowNone=True)
        self.avatarId = avatarId
        self.iters = getStat('terane.protocols.xmlrpc.iter.count', 0)
        self.totalitertime = getStat('terane.protocols.xmlrpc.iter.totaltime', 0.0)
        self.tails = getStat('terane.protocols.xmlrpc.tail.count', 0)
        self.totaltailtime = getStat('terane.protocols.xmlrpc.tail.totaltime', 0.0)

    def _handleError(self, failure):
        try:
            raise failure
        except xmlrpclib.Fault:
            raise
        except (QuerySyntaxError, QueryExecutionError), e:
            raise FaultBadRequest(e)
        except BaseException, e:
            logger.exception(failure)
            raise FaultInternalError()

    @inlineCallbacks
    def xmlrpc_iter(self, query, last=None, indices=None, limit=100, reverse=False, fields=None):
        try:
            if indices == None:
                result = yield self.queries.listIndices()
            indices = [i for i in result.data \
              if self.auth.canAccess(self.avatarId, 'index', i, 'PERM::XMLRPC::ITER')]
            if indices == []:
                raise FaultNotAuthorized("not authorized to access the specified resource")
            self.iters += 1
            result = yield self.queries.iter(unicode(query), last, indices, limit, reverse, fields)
            self.totalitertime += float(result.meta['runtime'])
            returnValue(result)
        except Exception, e:
            self._handleError(e)

    @inlineCallbacks
    def xmlrpc_tail(self, query, last=None, indices=None, limit=100, fields=None):
        try:
            if indices == None:
                result = yield self.queries.listIndices()
            indices = [i for i in result.data \
              if self.auth.canAccess(self.avatarId, 'index', i, 'PERM::XMLRPC::TAIL')]
            if indices == []:
                raise FaultNotAuthorized("not authorized to access the specified resource")
            self.tails += 1
            result = yield self.queries.tail(unicode(query), last, indices, limit, fields)
            self.totaltailtime += float(result.meta['runtime'])
            returnValue(result)
        except Exception, e:
            self._handleError(e)

    @inlineCallbacks
    def xmlrpc_listIndices(self):
        try:
            result = yield self.queries.listIndices()
            indices = [i for i in result.data \
              if self.auth.canAccess(self.avatarId, 'index', i, 'PERM::XMLRPC::LISTINDEX')]
            if indices == []:
                raise FaultNotAuthorized("not authorized to access the specified resource")
            result.data = indices
            returnValue(result)
        except Exception, e:
            self._handleError(e)

    def xmlrpc_showIndex(self, name):
        try:
            if not self.auth.canAccess(self.avatarId, 'index', name, 'PERM::XMLRPC::SHOWINDEX'):
                raise FaultNotAuthorized("not authorized to access the specified resource")
            return self.queries.showIndex(name).addErrback(self._handleError)
        except BaseException, e:
            self._handleError(e)

    def xmlrpc_showStats(self, name, recursive=False):
        try:
            return self.stats.showStats(name, recursive)
        except BaseException, e:
            self._handleError(e)

    def xmlrpc_flushStats(self, flushAll=False):
        try:
            self.stats.flushStats(flushAll)
            return ({}, [])
        except BaseException, e:
            self._handleError(e)

class XMLRPCRealm(object):
    implements(IRealm)

    def requestAvatar(self, avatarId, mind, *interfaces):
        logger.debug("logged in as %s" % avatarId)
        if IResource in interfaces:
            return (IResource, XMLRPCDispatcher(avatarId), lambda: None)
        raise NotImplementedError()

class XMLRPCProtocol(Protocol):
    implements(IProtocol)

    def getDefaultPort(self):
        return 45565

    def makeFactory(self):
        registry = getRegistry()
        XMLRPCDispatcher.auth = registry.getComponent(IManager, 'auth')
        XMLRPCDispatcher.queries = registry.getComponent(IManager, 'queries')
        XMLRPCDispatcher.stats = registry.getComponent(IManager, 'stats')
        portal = XMLRPCDispatcher.auth.getPortal(XMLRPCRealm())
        credentialFactory = BasicCredentialFactory("terane")
        wrapper = HTTPAuthSessionWrapper(portal, [credentialFactory])
        return Site(wrapper)

class XMLRPCProtocolPlugin(Plugin):
    implements(IPlugin)
    components = [(XMLRPCProtocol, IProtocol, 'xmlrpc')]
