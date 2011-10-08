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
from terane.loggers import getLogger

logger = getLogger('terane.inputs.syslog')

class SyslogUDPReceiver(DatagramProtocol):
    
    def __init__(self, plugin):
        self._plugin = plugin

    def datagramReceived(self, data, (host,port)):
        try:
            fields = {'_raw': data, '_host': host, '_port': port}
            logger.trace("received msg from %s:%i: %s" % (host,port,data))
            for input in self._plugin:
                if input._check(host, port):
                    input._write(fields)
        except Exception, e:
            logger.debug(str(e))

class SyslogInput(Input):

    implements(IInput)

    def configure(self, section):
        allowed = section.getString('syslog udp allowed clients', '').strip()

    def outfields(self):
        return set(('_raw', '_host', '_port'))

    def startService(self):
        Input.startService(self)
        logger.debug("[input:%s] started input" % self.name)

    def _check(self, host, port):
        # return True if host:port passes access restrictions
        return True

    def _write(self, fields):
        fields['input'] = self.name
        self.on_received_event.signal(fields)

    def stopService(self):
        Input.stopService(self)
        logger.debug("[input:%s] stopped input" % self.name)
        
class SyslogInputPlugin(Plugin):

    implements(IPlugin)

    factory = SyslogInput

    def configure(self, section):
        self._udplistener = None
        self._udpaddress = section.getString('syslog udp address', '0.0.0.0')
        self._udpport = section.getInt('syslog udp port', 514)

    def startService(self):
        Plugin.startService(self)
        if len(self.services) > 0:
            receiver = SyslogUDPReceiver(self)
            self._udplistener = reactor.listenUDP(self._udpport, receiver, interface=self._udpaddress)
            logger.info("[plugin:input:syslog] listening for udp syslog messages on %s:%i" % (self._udpaddress,self._udpport))
        else:
            logger.info("[plugin:input:syslog] no syslog inputs configured")

    def stopService(self):
        if not self._udplistener == None:
            self._udplistener.stopListening()
            self._udplistener = None
        Plugin.stopService(self)
        logger.info("[plugin:input:syslog] stopped listening for syslog messages")


