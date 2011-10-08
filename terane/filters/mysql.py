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

logger = getLogger("terane.filters.mysql")

class MysqlServerFilter(Filter):

    implements(IFilter)

    def configure(self, section):
        self._regex = re.compile(r'(?P<date>\d{6})\w+(?P<time>\d\d:\d\d\:\d\d)\w+(?P<msg>.*)')

    def infields(self):
        # this filter requires the following incoming fields
        return set(('_raw',))

    def outfields(self):
        # this filter guarantees values for the following outgoing fields
        return set()

    def filter(self, fields):
        # if the regex matches, then we have a timestamped event
        m = self._regex.match(fields['_raw'])
        if m != None:
            try:
                # override the default timestamp
                fields['ts'] = datetime.datetime.strptime("%s %s" % 
                    m.group('date','time'), '%y%m%d %%H:%M:%S')
            except Exception, e:
                raise FilterError("failed to parse timestamp: %s" % e)
            # put the rest of the line into the default field
            fields['default'] = m.group('msg')
        # otherwise just stick the entire line into the default field
        else:
            fields['default'] = fields['_raw']
        return fields

class MysqlServerFilterPlugin(Plugin):
    implements(IPlugin)
    factory = MysqlServerFilter()
