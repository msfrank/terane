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

import re, time, dateutil.parser
from terane.plugins import Plugin
from terane.filters import Filter, FilterError, StopFiltering
from terane.loggers import getLogger

logger = getLogger("terane.filters.syslog")

class SyslogFilterPlugin(Plugin):

    def configure(self, section):
        pass

    def instance(self):
        return SyslogFilter()

class SyslogFilter(Filter):

    def _updateselected(self, selector):
            # split the selector into the facility list and serverity
            facilities,severity = selector.split('.', 1)
            # parse the facility list
            if facilities == '*':
                facilities = _facilities.values()
            else:
                try:
                    facilities = [_facilities[f] for f in facilities.split(',') if not f == '']
                except KeyError, e:
                    raise InputError("[filter:%s] selector %s has invalid facility %s" % (self.name,selector,e))
                except Exception, e:
                    raise InputError("[filter:%s] failed to parse facilities for selector %s: %s" % (self.name,selector,e))
            # parse the severity, first checking for a modifier
            if severity[0] in ('=','!'):
                modifier = severity[0]
                severity = serverity[1:]
            else:
                modifier = None
            # log msgs of all severities for the specified facilities
            if severity == '*':
                addset = set()
                for facility in facilities:
                    for s in range(0, 8):
                        addset.add((facility * 8) + s)
                self._selected |= addset
            # ignore msgs of all severities for the specified facilities
            elif severity == 'none':
                delset = set()
                for facility in facilities:
                    for s in range(0, 8):
                        delset.add((facility * 8) + s)
                self._selected -= delset
            else:
                if severity not in _severities:
                    raise InputError("[filter:%s] selector %s has invalid severity %s" % (self.name,selector,severity))
                severity = _severities[severity]
                # log msgs of the specified severity for the specified facilities
                if modifier == '=':
                    addset = set()
                    for facility in facilities:
                        addset.add((facility * 8) + severity)
                    self._selected |= addset
                # ignore msgs of the specified severity for the specified facilities
                elif modifier == '!=':
                    delset = set()
                    for facility in facilities:
                        addset.add((facility * 8) + severity)
                    self._selected -= delset
                # ignore msgs of equal or greater importance than the specified severity for the specified facilities
                elif modifier == '!':
                    delset = set()
                    for facility in facilities:
                        for s in range(0, severity + 1):
                            delset.add((facility * 8) + s)
                    self._selected -= delset
                # log msgs of equal or greater importance than the specified severity for the specified facilities
                else:
                    addset = set()
                    for facility in facilities:
                        for s in range(0, severity + 1):
                            addset.add((facility * 8) + s)
                    self._selected |= addset

    def configure(self, section):
        self._filematcher = re.compile(r'(?P<ts>[A-Za-z]{3} [ \d]\d \d\d:\d\d\:\d\d) (?P<hostname>\S*) (?P<default>.*)')
        self._PRImatcher = re.compile(r'<(?P<pri>[0-9]{1,3})>')
        self._TIMESTAMPmatcher = re.compile(r'(?P<timestamp>[A-Za-z]{3} [ \d]\d \d\d:\d\d\:\d\d )')
        self._selected = set()
        # parse each selector, separated by semicolons
        selectors = section.getString('syslog selectors', '').strip()
        if not selectors == '':
            for selector in [s.strip() for s in selectors.split(';') if not s == '']:
                self._updateselected(selector)

    def infields(self):
        # this filter requires the following incoming fields
        return set(('_raw',))

    def outfields(self):
        # this filter guarantees values for the following outgoing fields
        return set()

    def filter(self, fields):
        if fields['_raw'][0].isdigit():
            return self._filterSyslog(fields)
        return self._filterFile(fields)

    def _filterFile(self, fields):
        m = self._filematcher.match(fields['_raw'])
        if m == None:
            raise FilterError("[filter:%s] line '%s' is not in syslog format" % (self.name,line))
        ts,hostname,default = m.group('ts','hostname','default')
        if ts == None or hostname == None or default == None:
            raise FilterError("[filter:%s] line '%s' is not in syslog format" % (self.name,line))
        # parse the timestamp
        try:
            fields['ts'] = dateutil.parser.parse(ts)
        except Exception, e:
            raise FilterError("[filter:%s] failed to parse ts '%s': %s" % (self.name, ts, e))
        fields['hostname'] = hostname
        fields['default'] = default
        return fields

    def _filterSyslog(self, fields):
        data = fields['_raw']
        # parse the PRI section
        m = self._PRImatcher.match(data)
        if m == None:
            raise FilterError("[filter:%s] line has invalid PRI section" % self.name)
        pri = m.group('pri')
        # trim the PRI section off
        data = data[len(pri)+2:]
        # make sure that the PRI value is in decimal (a leading '0' would indicate octal)
        if len(pri) > 1 and pri[0] == '0':
            raise FilterError("[filter:%s] line has invalid PRI section" % self.name)
        # verify that we are interested in this particular priority
        pri = int(pri)
        if not pri in self._selected:
            raise StopFiltering("[filter:%s] not interested in msg with priority %i" % (self.name,pri))
        # parse the facility and severity from the priority
        facility = pri / 8
        severity = pri % 8
        if facility < 0 or facility > 23:
            raise FilterError("[filter:%s] line has invalid facility %i" % (self.name,facility))
        if severity < 0 or severity > 7:
            raise FilterError("[filter:%s] line has invalid severity %i" % (self.name,severity))
        # notify each input about the new syslog message
        fields['facility'] = facility
        fields['severity'] = severity
        # parse the HEADER section
        # this is a RFC5424-compliant syslog message
        if data[0].isdigit():
            raise FilterError("[filter:%s] can't parse RFC5424 syslog messages" % self.name)
        # this is a BSD syslog message
        m = self._TIMESTAMPmatcher.match(data)
        if m == None:
            raise FilterError("[filter:%s] line has an invalid timestamp" % self.name)
        timestamp = m.group('timestamp')
        if timestamp == None:
            raise FilterError("[filter:%s] line has an invalid timestamp" % self.name)
        # trim the TIMESTAMP section off
        data = data[len(timestamp):]
        # parse the timestamp
        try:
            fields['ts'] = dateutils.parser.parse(timestamp).isoformat()
        except Exception, e:
            raise FilterError("[filter:%s] failed to parse date '%s': %s" % (self.name, timestamp, e))
        # the remainder of the data consists of the HOSTNAME, a space, then the MSG
        try:
            hostname,default = data.split(' ', 1)
        except:
            raise FilterError("[filter:%s] line has no syslog body")
        fields['hostname'] = hostname
        fields['default'] = default
        return fields

_severities = {
    'emerg': 0,
    'alert': 1,
    'crit': 2,
    'err': 3,
    'warning': 4,
    'notice': 5,
    'info': 6,
    'debug': 7
    }

_facilities = {
    'kern': 0,          # kernel messages
    'user': 1,          # user-level messages
    'mail': 2,          # mail system
    'daemon': 3,        # system daemons
    'auth': 4,          # security/authorization messages (see RFC 3164, section 4.1.1, table 1, note 1)
    'syslog': 5,        # messages generated internally by syslogd
    'lpr': 6,           # line printer subsystem
    'news': 7,          # network news subsystem
    'uucp': 8,          # UUCP subsystem
    'cron': 9,          # clock daemon (see RFC 3164, section 4.1.1, table 1, note 2)
    'authpriv': 10,     # security/authorization messages (see RFC 3164, section 4.1.1, table 1, note 1)
    'ftp': 11,          # FTP daemon
    'ntp': 12,          # NTP subsystem
    'auth': 13,         # log audit (see RFC 3164, section 4.1.1, table 1, note 1)
    'auth': 14,         # log alert (see RFC 3164, section 4.1.1, table 1, note 1)
    'cron': 15,         # clock daemon (see RFC 3164, section 4.1.1, table 1, note 2)
    'local0': 16,       # local use 0  (local0)
    'local1': 17,       # local use 1  (local1)
    'local2': 18,       # local use 2  (local2)
    'local3': 19,       # local use 3  (local3)
    'local4': 20,       # local use 4  (local4)
    'local5': 21,       # local use 5  (local5)
    'local6': 22,       # local use 6  (local6)
    'local7': 23        # local use 7  (local7)
    }
