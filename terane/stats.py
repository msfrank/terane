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

import os
from twisted.application.service import Service
from twisted.internet import task
from terane.loggers import getLogger

logger = getLogger('terane.stats')

class Stat(object):

    def __init__(self, name, initial, itype):
        self.name = name
        self._value = itype(initial)
        self._itype = itype

    def _getvalue(self):
        return self._value

    def _setvalue(self, v):
        self._value = self._itype(v)

    value = property(_getvalue,_setvalue)
    
class StatsManager(Service):

    def __init__(self):
        self._stats = {}

    def configure(self, settings):
        section = settings.section('server')
        self.statsfile = section.getString('stats file', None)
        self.syncinterval = section.getInt('stats sync interval', 10)
        if self.syncinterval <= 0:
            self.syncinterval = 1

    def startService(self):
        Service.startService(self)
        if self.statsfile:
            self._syncstats = task.LoopingCall(self.save)
            self._syncstats.start(self.syncinterval * 60, False)
        else:
            self._syncstats = None

    def stopService(self):
        if self._syncstats:
            self._syncstats.stop()
            self._syncstats = None
            stats.save()
        Service.stopService(self)

    def get(self, name, initial, itype):
        for c in name.split('.'):
            if not c.isalnum():
                raise ValueError("'name' must consist of only letters, numbers, and periods")
        if name in self._stats:
            s = self._stats[name]
            if not s._itype == itype:
                raise TypeError("%s already has type %s" % (name,s._itype.__name__))
        else:
            s = Stat(name, initial, itype)
            self._stats[name] = s
        return s

    def save(self):
        if self.statsfile == None:
            return
        try:
            with open(self.statsfile, 'w') as f:
                for name,s in sorted(self._stats.items(), lambda x,y: cmp(x[0],y[0])):
                    f.write("%s %s\n" % (name,str(s.value)))
        except (IOError,OSError), e:
            logger.warning("failed to save statistics: %s" % e.strerror)
        except Exception, e:
            logger.warning("failed to save statistics: %s" % str(e))


stats = StatsManager()
