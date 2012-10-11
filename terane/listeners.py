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

from zope.interface import implements
from twisted.application.service import Service
from twisted.internet.protocol import ServerFactory
from terane.manager import IManager, Manager
from terane.plugins import IPluginStore
from terane.auth import IAuthManager
from terane.queries import IQueryManager
from terane.protocols import IProtocol
from terane.settings import ConfigureError
from terane.loggers import getLogger

logger = getLogger('terane.listeners')

class Listener(Service):

    def __init__(self, name, protocol):
        self.setName(name)
        self._protocol = protocol

    def configure(self, section):
        self.listenAddress = section.getString("listen address", "0.0.0.0")
        self.listenPort = section.getInt("listen port", self._protocol.getDefaultPort())
        self.listenBacklog = section.getInt("listen backlog", 50)

    def startService(self):
        protoFactory = self._protocol.makeFactory()
        if not isinstance(protoFactory, ServerFactory):
            raise TypeError("protocol returned an unsuitable server factory")
        from twisted.internet import reactor
        self.listener = reactor.listenTCP(self.listenPort, protoFactory,
            self.listenBacklog, self.listenAddress)
        logger.info("[listener:%s] started listening on %s:%i" % 
            (self.name, self.listenAddress, self.listenPort))

    def stopService(self):
        self.listener.stopListening()
        self.listener = None
        logger.info("[listener:%s] stopped listening" % self.name)

class ListenerManager(Manager):
    """
    """

    implements(IManager)

    def __init__(self, pluginstore, authmanager, querymanager):
        if not IPluginStore.providedBy(pluginstore):
            raise TypeError("authmanager class does not implement IAuthManager")
        if not IAuthManager.providedBy(authmanager):
            raise TypeError("authmanager class does not implement IAuthManager")
        if not IQueryManager.providedBy(querymanager):
            raise TypeError("querymanager class does not implement IQueryManager")
        Manager.__init__(self)
        self.setName("listeners")
        self._pluginstore = pluginstore
        self._authmanager = authmanager
        self._querymanager = querymanager
        self._listeners = {}

    def configure(self, settings):
        """
        """
        for section in settings.sectionsLike("listener:"):
            name = section.name.split(':',1)[1]
            type = section.getString('type', None)
            if type == None:
                raise ConfigureError("listener %s missing required parameter 'type'" % name)
            try:
                factory = self._pluginstore.getComponent(IProtocol, type)
            except KeyError:
                raise ConfigureError("no protocol found for type '%s'" % type)
            protocol = factory(self._authmanager, self._querymanager)
            listener = Listener(name, protocol)
            listener.setServiceParent(self)
            try:
                listener.configure(section)
            except ConfigureError:
                listener.disownServiceParent()
                raise
            except Exception, e:
                listener.disownServiceParent()
                logger.exception(e)
                logger.warning("failed to load listener '%s'" % lname)
