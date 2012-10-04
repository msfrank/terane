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

import os, sys
from twisted.internet import reactor
from twisted.spread.pb import PBServerFactory, IPerspective, Avatar
from twisted.cred.portal import Portal, IRealm
from twisted.cred.checkers import AllowAnonymousAccess
from twisted.cred.error import Unauthorized
from twisted.internet.defer import Deferred
from zope.interface import implements
from terane.plugins import Plugin, IPlugin
from terane.inputs import Input, IInput
from terane.loggers import getLogger

logger = getLogger('terane.inputs.collect')

class Collector(Avatar):
    
    def __init__(self, avatarId, plugin):
        self._id = avatarId
        self._plugin = plugin

    def perspective_collect(self, event):
        try:
            logger.debug("collected remote event from %s: %s" % (self._id,event))
            for input in self._plugin._inputs:
                input._write(event.copy())
        except Exception, e:
            logger.debug(str(e))

class CollectorRealm:
    implements(IRealm)

    def __init__(self, plugin):
        self._plugin = plugin

    def requestAvatar(self, avatarId, mind, *interfaces):
        if IPerspective not in interfaces:
            raise Unauthorized()
        return IPerspective, Collector(avatarId, self._plugin), lambda:None
        
class CollectInput(Input):

    implements(IInput)

    def startService(self):
        Input.startService(self)
        logger.debug("[input:%s] started input" % self.name)

    def _write(self, event):
        self.on_received_event.signal(event)

    def stopService(self):
        Input.stopService(self)
        logger.debug("[input:%s] stopped input" % self.name)
        
class CollectInputPlugin(Plugin):

    implements(IPlugin)

    components = [(CollectInput, IInput, 'collect')]

    def configure(self, section):
        self._inputs = []
        self._listener = None
        self._address = section.getString('collect address', '0.0.0.0')
        self._port = section.getInt('collect port', 8643)

    def startService(self):
        Plugin.startService(self)
        if not self._inputs == []:
            portal = Portal(CollectorRealm(self))
            portal.registerChecker(AllowAnonymousAccess())
            factory = PBServerFactory(portal)
            self._listener = reactor.listenTCP(self._port, factory, interface=self._address)
            logger.info("[%s] listening for remote messages on %s:%i" % (self.name,self._address,self._port))
        else:
            logger.info("[%s] no inputs configured" % self.name)

    def stopService(self):
        if not self._listener == None:
            self._listener.stopListening()
            self._listener = None
        Plugin.stopService(self)
        logger.info("[%s] stopped listening for remote messages" % self.name)


