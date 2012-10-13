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
from zope.interface import implements
from terane.plugins import Plugin, IPlugin
from terane.filters import Filter, IFilter, FilterError
from terane.bier.event import Contract
from terane.loggers import getLogger

logger = getLogger("terane.filters.syslog")

class SyslogFilter(Filter):

    implements(IFilter)

    def configure(self, section):
        self._linematcher = re.compile(r'(?P<ts>[A-Za-z]{3} [ \d]\d \d\d:\d\d\:\d\d) (?P<hostname>\S*) (?P<msg>.*)')
        self._tagmatcher = re.compile(r'^(\S+)\[(\d+)\]:$|^(\S+):$')
        self._contract = Contract()
        self._contract.addAssertion('syslog_pid', 'literal', guarantees=False)
        self._contract.addAssertion('syslog_tag', 'text', guarantees=False)
        self._contract.sign()

    def getContract(self):
        return self._contract

    def filter(self, event):
        # split the line into timestamp, hostname, and message
        m = self._linematcher.match(event[self._contract.field_message])
        if m == None:
            raise FilterError("[filter:%s] line is not in syslog format" % self.name)
        ts,hostname,msg = m.group('ts','hostname','msg')
        if ts == None or hostname == None or msg == None:
            raise FilterError("[filter:%s] line is not in syslog format" % self.name)
        # parse the timestamp
        try:
            event.ts = dateutil.parser.parse(ts)
        except Exception, e:
            raise FilterError("[filter:%s] failed to parse ts '%s': %s" % (self.name, ts, e))
        event[self._contract.field_hostname] = hostname
        # split the message into tag and content
        tag,content = msg.split(' ', 1)
        m = self._tagmatcher.match(tag)
        if m == None:
            raise FilterError("[filter:%s] line has an invalid tag" % self.name)
        data = m.groups()
        if data[0] != None and data[1] != None:
            event[self._contract.field_syslog_tag] = data[0]
            event[self._contract.field_syslog_pid] = data[1]
        elif data[2] != None:
            event[self._contract.field_syslog_tag] = data[2]
        else:
            raise FilterError("[filter:%s] line has an invalid tag" % self.name)
        event[self._contract.field_message] = content
        return event

class SyslogFilterPlugin(Plugin):
    implements(IPlugin)
    components = [(SyslogFilter, IFilter, 'syslog')]
