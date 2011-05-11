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

import re
from terane.plugins import Plugin
from terane.filters import Filter, FilterError, StopFiltering
from terane.loggers import getLogger

logger = getLogger("terane.filters.regex")

class RegexFilterPlugin(Plugin):

    def configure(self, section):
        pass

    def instance(self):
        return RegexFilter()


class RegexFilter(Filter):

    def configure(self, section):
        self._infield = section.getString('regex field', '_raw')
        regex = section.getString('regex pattern', None)
        if regex == None:
            raise ConfigureError("[filter:%s] missing required parameter 'regex pattern'" % self.name)
        try:
            self._regex = re.compile(regex)
        except Exception, e:
            raise ConfigureError("[filter:%s] failed to compile regex pattern '%s': %s" % (self.name,regex,str(e)))
        self._outfields = self._regex.groupindex.keys()

    def infields(self):
        # this filter requires the following incoming fields
        return set((self._infield,))

    def outfields(self):
        # this filter guarantees values for the following outgoing fields
        return set(self._outfields)

    def filter(self, fields):
        if not self._infield in fields:
            raise FilterError("input is missing '%s' field" % self._infield)
        m = self.regex.match(fields[self._infield])
        if m == None:
            raise StopFiltering("input '%s' didn't match regex" % fields[self._infield])
        fields.update(m.groupdict())
        return fields
