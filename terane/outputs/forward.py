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
from twisted.internet.defer import Deferred
from twisted.spread.pb import PBClientFactory, DeadReferenceError
from twisted.cred.credentials import Anonymous
from twisted.internet import reactor
from twisted.python.failure import Failure
from zope.interface import implements
from terane.plugins import Plugin, IPlugin
from terane.outputs import Output, IOutput
from terane.loggers import getLogger
from terane.stats import getStat

logger = getLogger('terane.outputs.forward')

class ForwardOutput(Output):

    implements(IOutput)

    def configure(self, section):
        self.forwardserver = section.getString('forwarding address', None)
        self.forwardport = section.getInt('forwarding port', None)
        self.retryinterval = section.getInt('retry interval', 10)
        self.forwardedevents = getStat("terane.output.%s.forwardedevents" % self.name, 0)
        self.stalerefs = getStat("terane.output.%s.stalerefs" % self.name, 0)
        
    def startService(self):
        Output.startService(self)
        self._client = None
        self._listener = None
        self._remote = None
        self._backoff = None
        self._reconnect()

    def _reconnect(self):
        try:
            if self._client:
                self._client.disconnect()
            self._client = PBClientFactory()
            if self._listener:
                self._listener.disconnect()
            self._listener = reactor.connectTCP(self.forwardserver, self.forwardport, self._client)
            self._remote = self._client.login(Anonymous())
            self._remote.addCallback(self._login)
            self._remote.addErrback(self._loginFailed)
        except Exception, e:
            logger.error("[output:%s] failed to connect to remote collector: %s" % (self.name,str(e)))
            logger.error("[output:%s] will retry to connect in %i seconds" % (self.name,self.retryinterval))
            self._backoff = reactor.callLater(self.retryinterval, self._reconnect)

    def _login(self, remote):
        self._remote = remote
        self._backoff = None
        logger.debug("[output:%s] connected to remote collector" % self.name)

    def _loginFailed(self, reason):
        logger.error("[output:%s] failed to login to remote collector: %s" % (self.name,str(reason.value)))
        logger.error("[output:%s] will retry to connect in %i seconds" % (self.name,self.retryinterval))
        self._backoff = reactor.callLater(self.retryinterval, self._reconnect)

    def stopService(self):
        Output.stopService(self)
        if self._client:
            self._client.disconnect()
        self._client = None
        if self._listener:
            self._listener.disconnect()
        self._listener = None
        self._client = None
        self._remote = None
        self._backoff = None

    def receiveEvent(self, fields):
        try:
            logger.debug("[output:%s] forwarding event: %s" % (self.name,str(fields)))
            d = self._remote.callRemote('collect', fields)
            d.addCallback(self._collected)
            d.addErrback(self._collectFailed)
        except DeadReferenceError:
            # if we are not already in the process of reconnecting, then reconnect
            if not isinstance(self._remote, Deferred):
                self.stalerefs += 1
                logger.debug("[output:%s] lost reference to collector at %s:%i" %
                (self.name, self.forwardserver, self.forwardport))
                self._reconnect()

    def _collected(self, unused):
        self.forwardedevents += 1

    def _collectFailed(self, reason):
        logger.debug("[output:%s] failed to forward event to %s:%i: %s" %
            (self.name, self.forwardserver, self.forwardport, str(reason)))
        return reason

class ForwardOutputPlugin(Plugin):
    implements(IPlugin)
    components = [(ForwardOutput, IOutput, 'forward')]
