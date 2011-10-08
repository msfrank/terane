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
from terane.loggers import getLogger

logger = getLogger("terane.filters.apache")

class ApacheCommonFilter(Filter):

    implements(IFilter)

    def configure(self, section):
        self._regex = re.compile(r'''
                (?P<remotehost>[\d.]+)\ 
                (?P<remotelog>\S+)\ 
                (?P<remoteuser>\S+)\ 
                \[(?P<date>[\w:/]+\s[+\-]\d{4})\]\ 
                \"(?P<request>.+?)\"\ 
                (?P<status>\d{3})\ 
                (?P<byteswritten>\d+)''',
            re.VERBOSE
            )
        self._outfields = ('remotehost', 'remotelog', 'remoteuser', 'request',
            'status', 'byteswritten')

    def infields(self):
        # this filter requires the following incoming fields
        return set(('_raw',))

    def outfields(self):
        # this filter guarantees values for the following outgoing fields
        return set(self._outfields)

    def filter(self, fields):
        m = self._regex.match(fields['_raw'])
        fields['default'] = fields['_raw']
        if m == None:
            raise FilterError("incoming line '%s' didn't match regex" % line)
        date = m.group('date')
        if date == None:
            raise FilterError("regex did not match 'date'")
        # parse the timestamp
        try:
            fields['ts'] = dateutil.parser.parse(date, dayfirst=True, fuzzy=True).isoformat()
        except Exception, e:
            raise FilterError("failed to parse date '%s': %s" % (date, e))
        # extract each field
        for field in self._outfields:
            value = m.group(field)
            if value == None:
                raise FilterError("regex did not match '%s'" % field)
            fields[field] = value
        return fields

class ApacheCommonFilterPlugin(Plugin):
    implements(IPlugin)
    factory = ApacheCommonFilter

class ApacheCombinedFilter(Filter):

    implements(IFilter)

    def configure(self, section):
        self._regex = re.compile(r'''
                (?P<remotehost>[\d.]+)\ 
                (?P<remotelog>\S+)\ 
                (?P<remoteuser>\S+)\ 
                \[(?P<date>[\w:/]+\s[+\-]\d{4})\]\ 
                \"(?P<request>.+?)\"\ 
                (?P<status>\d{3})\ 
                (?P<byteswritten>\d+)\ 
                \"(?P<referrer>[^\"]+)\"\ 
                \"(?P<useragent>[^\"]+)\"''',
            re.VERBOSE
            )
        self._outfields = ('remotehost', 'remotelog', 'remoteuser', 'request',
            'status', 'byteswritten', 'referrer', 'useragent')

    def infields(self):
        # this filter requires the following incoming fields
        return set(('_raw',))

    def outfields(self):
        # this filter guarantees values for the following outgoing fields
        return set(self._outfields)

    def filter(self, fields):
        m = self._regex.match(fields['_raw'])
        fields['default'] = fields['_raw']
        if m == None:
            raise FilterError("incoming line '%s' didn't match regex" % fields['_raw'])
        date = m.group('date')
        if date == None:
            raise FilterError("regex did not match 'date'")
        # parse the timestamp
        try:
            fields['ts'] = dateutil.parser.parse(date, dayfirst=True, fuzzy=True).isoformat()
        except Exception, e:
            raise FilterError("failed to parse date '%s': %s" % (date, e))
        # extract each field
        for field in self._outfields:
            value = m.group(field)
            if value == None:
                raise FilterError("regex did not match '%s'" % field)
            fields[field] = value
        return fields

class ApacheCombinedFilterPlugin(Plugin):
    implements(IPlugin)
    factory = ApacheCombinedFilter()
