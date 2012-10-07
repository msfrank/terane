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
from terane import IManager, Manager
from terane.registry import getRegistry
from terane.protocols import IProtocol
from terane.settings import ConfigureError
from terane.loggers import getLogger

logger = getLogger('terane.listeners')

class Listener(Service):

    def configure(self, section):
        ptype = section.getString('type', None)
        if ptype == None:
            raise ConfigureError("[listener:%s] missing required option 'type'" % self.name)
        try:
            registry = getRegistry()
            factory = registry.getComponent(IProtocol, ptype)
            self.protocol = factory()
        except:
            raise ConfigureError("no protocol named '%s'" % ptype)
        self.listenAddress = section.getString("listen address", "0.0.0.0")
        self.listenPort = section.getInt("listen port", self.protocol.getDefaultPort())
        self.listenBacklog = section.getInt("listen backlog", 50)

    def startService(self):
        protoFactory = self.protocol.makeFactory()
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

    def __init__(self):
        Manager.__init__(self)
        self.setName("listeners")
        self._listeners = {}

    def configure(self, settings):
        """
        """
        registry = getRegistry()
        for section in settings.sectionsLike("listener:"):
            lname = section.name.split(':',1)[1]
            listener = Listener()
            listener.setName(lname)
            listener.setServiceParent(self)
            try:
                listener.configure(section)
                registry.addComponent(listener, Listener, lname)
            except ConfigureError:
                listener.disownServiceParent()
                raise
            except Exception, e:
                listener.disownServiceParent()
                logger.exception(e)
                logger.warning("failed to load listener '%s'" % lname)
