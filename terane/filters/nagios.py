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
from terane.plugins import Plugin
from terane.filters import Filter, FilterError, StopFiltering
from terane.loggers import getLogger

logger = getLogger("terane.filters.nagios")

class NagiosFilterPlugin(Plugin):

    def configure(self, section):
        pass

    def instance(self):
        return NagiosFilter()

class NagiosFilter(Filter):

    def configure(self, section):
        pass

    def infields(self):
        # this filter requires the following incoming fields
        return set(('_raw',))

    def outfields(self):
        # this filter guarantees values for the following outgoing fields
        return set(('nagios_evtype',))

    def filter(self, fields):
        line = fields['_raw']
        # all lines should start with '['
        if line[0] != '[':
            raise FilterError("incoming line '%s' didn't start with timestamp" % line)
        # parse the event timestamp
        try:
            ts,line = line[1:].split(']', 1)
        except:
            raise FilterError("incoming line '%s' didn't start with timestamp" % line)
        try:
            fields['ts'] = datetime.fromtimestamp(float(ts))
        except Exception, e:
            raise FilterError("%s cannot be converted into a timestamp: %s" % (ts, e))
        # determine the event type
        try:
            evtype,line = line.split(':', 1)
            evtype = evtype.strip()
        except:
            raise StopFiltering()
        # set the nagios_event type field
        fields['nagios_event'] = evtype
        # parse the rest of the line
        if evtype == 'HOST ALERT':
            return self._hostAlert(line, fields)
        if evtype == 'SERVICE ALERT':
            return self._serviceAlert(line, fields)
        if evtype == 'Error':
            return self._error(line, fields)
        if evtype == 'Warning':
            return self._warning(line, fields)
        raise StopFiltering()
        
    def _hostAlert(self, line, fields):
        try:
            host,status,state,attempt,detail = line.strip().split(';', 4)
            fields['nagios_host'] = host
            fields['nagios_status'] = status
            fields['nagios_state'] = state
            fields['nagios_attempt'] = attempt
            fields['default'] = detail
            return fields
        except Exception, e:
            FilterError("failed to parse host alert: %s" % e)

    def _serviceAlert(self, line, fields):
        try:
            host,service,status,state,attempt,detail = line.strip().split(';', 5)
            fields['nagios_host'] = host
            fields['nagios_service'] = service
            fields['nagios_status'] = status
            fields['nagios_state'] = state
            fields['nagios_attempt'] = attempt
            fields['default'] = detail
            return fields
        except Exception, e:
            FilterError("failed to parse service alert: %s" % e)

    def _error(self, line, fields):
        fields['default'] = line.strip()
        return fields

    def _warning(self, line, fields):
        fields['default'] = line.strip()
        return fields
