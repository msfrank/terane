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

from datetime import datetime
from zope.interface import implements
from terane.plugins import Plugin, IPlugin
from terane.filters import Filter, IFilter, FilterError, StopFiltering
from terane.bier.event import Contract
from terane.loggers import getLogger

logger = getLogger("terane.filters.nagios")

class NagiosFilter(Filter):

    implements(IFilter)

    def configure(self, section):
        self._contract = Contract()
        self._contract.addAssertion('nagios_evtype', 'literal', guarantees=True)
        self._contract.addAssertion('nagios_host', 'text', guarantees=False)
        self._contract.addAssertion('nagios_service', 'text', guarantees=False)
        self._contract.addAssertion('nagios_status', 'text', guarantees=False)
        self._contract.addAssertion('nagios_state', 'text', guarantees=False)
        self._contract.addAssertion('nagios_attempt', 'text', guarantees=False)
        self._contract.sign()

    def getContract(self):
        return self._contract

    def filter(self, event):
        line = event[self._contract.field_message]
        # all lines should start with '['
        if line[0] != '[':
            raise FilterError("incoming line '%s' didn't start with timestamp" % line)
        # parse the event timestamp
        try:
            ts,line = line[1:].split(']', 1)
        except:
            raise FilterError("incoming line '%s' didn't start with timestamp" % line)
        try:
            event.ts = datetime.fromtimestamp(float(ts))
        except Exception, e:
            raise FilterError("%s cannot be converted into a timestamp: %s" % (ts, e))
        # determine the event type
        try:
            evtype,line = line.split(':', 1)
            evtype = evtype.strip()
        except:
            raise StopFiltering()
        # set the nagios_event type field
        event[self._contract.field_nagios_evtype] = evtype
        # parse the rest of the line
        if evtype == 'HOST ALERT':
            return self._hostAlert(line, event)
        if evtype == 'SERVICE ALERT':
            return self._serviceAlert(line, event)
        if evtype == 'Error':
            return self._error(line, event)
        if evtype == 'Warning':
            return self._warning(line, event)
        raise StopFiltering()
        
    def _hostAlert(self, line, event):
        try:
            host,status,state,attempt,detail = line.strip().split(';', 4)
            event[self._contract.field_nagios_host] = host
            event[self._contract.field_nagios_status] = status
            event[self._contract.field_nagios_state] = state
            event[self._contract.field_nagios_attempt] = attempt
            event[self._contract.field_message] = detail
            return event
        except Exception, e:
            FilterError("failed to parse host alert: %s" % e)

    def _serviceAlert(self, line, event):
        try:
            host,service,status,state,attempt,detail = line.strip().split(';', 5)
            event[self._contract.field_nagios_host] = host
            event[self._contract.field_nagios_service] = service
            event[self._contract.field_nagios_status] = status
            event[self._contract.field_nagios_state] = state
            event[self._contract.field_nagios_attempt] = attempt
            event[self._contract.field_message] = detail
            return event
        except Exception, e:
            FilterError("failed to parse service alert: %s" % e)

    def _error(self, line, event):
        event[self._contract.field_message] = detail
        return event

    def _warning(self, line, event):
        event[self._contract.field_message] = detail
        return event

class NagiosFilterPlugin(Plugin):
    implements(IPlugin)
    components = [(NagiosFilter, IFilter, 'nagios')]
