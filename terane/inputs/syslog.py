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

import os, sys, time, re
from twisted.internet import reactor
from twisted.internet.protocol import DatagramProtocol
from twisted.internet.defer import Deferred
from zope.interface import implements
from terane.plugins import Plugin, IPlugin
from terane.inputs import Input, IInput
from terane.signals import Signal
from terane.bier.event import Contract
from terane.loggers import getLogger

logger = getLogger('terane.inputs.syslog')

class SyslogUDPReceiver(DatagramProtocol):
    
    def __init__(self, plugin):
        self._plugin = plugin

    def datagramReceived(self, data, (host,port)):
        try:
            logger.trace("received msg from %s:%i: %s" % (host,port,data))
            for input in self._plugin:
                if input._check(host, port):
                    event = input._dispatcher.newEvent()
                    event[input._contract.field_hostname] = host
                    event[input._contract.field_message] = data
                    event[input._contract.field__raw] = data
                    event[input._contract.field__host] = data
                    event[input._contract.field__port] = data
                    input._dispatcher.signalEvent(event)
        except Exception, e:
            logger.debug(str(e))

class SyslogInput(Input):

    implements(IInput)

    def __init__(self, plugin):
        self._dispatcher = Signal()
        self._contract = Contract()
        self._contract.addAssertion('_host', 'text', expects=False, guarantees=True, ephemeral=True)
        self._contract.addAssertion('_port', 'literal', expects=False, guarantees=True, ephemeral=True)
        self._contract.addAssertion('_raw', 'literal', expects=False, guarantees=True, ephemeral=True)
        self._contract.sign()

    def configure(self, section):
        allowed = section.getString('syslog udp allowed clients', '').strip()

    def getContract(self):
        return self._contract

    def getDispatcher(self):
        return self._dispatcher

    def startService(self):
        Input.startService(self)
        logger.debug("[input:%s] started input" % self.name)

    def _check(self, host, port):
        # return True if host:port passes access restrictions
        return True

    def _write(self, event):
        self.on_received_event.signal(event)

    def stopService(self):
        Input.stopService(self)
        logger.debug("[input:%s] stopped input" % self.name)
        
class SyslogInputPlugin(Plugin):

    implements(IPlugin)

    components = [(SyslogInput, IInput, 'syslog')]

    def configure(self, section):
        self._udplistener = None
        self._udpaddress = section.getString('syslog udp address', '0.0.0.0')
        self._udpport = section.getInt('syslog udp port', 514)

    def startService(self):
        Plugin.startService(self)
        if len(self.services) > 0:
            receiver = SyslogUDPReceiver(self)
            self._udplistener = reactor.listenUDP(self._udpport, receiver, interface=self._udpaddress)
            logger.info("[%s] listening for udp syslog messages on %s:%i" % (self.name,self._udpaddress,self._udpport))
        else:
            logger.info("[%s] no syslog inputs configured" % self.name)

    def stopService(self):
        if not self._udplistener == None:
            self._udplistener.stopListening()
            self._udplistener = None
        Plugin.stopService(self)
        logger.info("[%s] stopped listening for syslog messages" % self.name)
