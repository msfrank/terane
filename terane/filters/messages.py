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
from terane.filters import Filter, FilterError
from terane.loggers import getLogger

logger = getLogger("terane.filters.messages")

class MessagesFilterPlugin(Plugin):

    def configure(self, section):
        pass

    def instance(self):
        return MessagesFilter()

class MessagesFilter(Filter):

    def configure(self, section):
        self._regex = re.compile(r'(?P<date>[A-Za-z]{3} [ \d]\d \d\d:\d\d\:\d\d) (?P<hostname>\S*) (?P<msg>.*)')

    def infields(self):
        # this filter requires the following incoming fields
        return set(('_raw',))

    def outfields(self):
        # this filter guarantees values for the following outgoing fields
        return set()

    def filter(self, fields):
        m = self._regex.match(fields['_raw'])
        if m == None:
            raise FilterError("incoming line '%s' didn't match regex" % line)
        date,hostname,default = m.group('date','hostname','msg')
        if date == None:
            raise FilterError("regex did not match 'date'")
        if hostname == None:
            raise FilterError("regex did not match 'hostname'")
        if default == None:
            raise FilterError("regex did not match 'msg'")
        # parse the timestamp
        try:
            fields['ts'] = dateutil.parser.parse(date).isoformat()
        except Exception, e:
            raise FilterError("failed to parse date '%s': %s" % (date, e))
        fields['hostname'] = hostname
        fields['default'] = default
        return fields
