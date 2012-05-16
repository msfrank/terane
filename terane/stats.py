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

import os, errno
from twisted.application.service import Service
from twisted.internet import task
from terane.settings import ConfigureError
from terane.loggers import getLogger

logger = getLogger('terane.stats')

class Stat(object):

    def __init__(self, name, ivalue, itype, volatile):
        self.name = name
        self._volatile = volatile
        self._ivalue = ivalue
        self._itype = itype
        self._value = itype(ivalue)

    def _getvalue(self):
        return self._value

    def _setvalue(self, v):
        self._value = self._itype(v)
        if self._volatile == False:
            stats._dirty = True
 
    def __add__(self, other):
        return self._getvalue() + self._itype(other)

    def __iadd__(self, other):
        self.value = self._getvalue() + self._itype(other)
        return self

    def __lshift__(self, other):
        return other

    def __ilshift__(self, other):
        self.value = other
        return self

    value = property(_getvalue,_setvalue)

class StatsManager(Service):

    def __init__(self):
        self._stats = {}
        self._dirty = False

    def configure(self, settings):
        section = settings.section('server')
        self.statsfile = section.getPath('stats file', '/var/lib/terane/statistics')
        self.syncinterval = section.getInt('stats sync interval', 1)
        if self.syncinterval <= 0:
            raise ConfigureError("stats sync interval must be greater than or equal to 0")

    def startService(self):
        Service.startService(self)
        if self.statsfile != '':
            self._loadStats()
            self._syncstats = task.LoopingCall(self._saveStats)
            self._syncstats.start(self.syncinterval, False)
            logger.debug("logging statistics to %s every %i seconds" % (self.statsfile,self.syncinterval)) 
        else:
            logger.debug("statistics logging is disabled")
            self._syncstats = None

    def stopService(self):
        if self._syncstats:
            self._syncstats.stop()
            self._syncstats = None
            self._saveStats()
        Service.stopService(self)

    def getStat(self, name, itype, ivalue, volatile):
        """
        """
        if itype == int:
            itype = long; ivalue = long(ivalue)
        if not itype == float and not itype == long and not itype == int:
            raise ValueError("%s is an unsupported type %s" % (name, itype.__name))
        if name.startswith('.') or name.endswith('.'):
            raise ValueError("'name' cannot start or end with a '.'")
        for c in name.split('.'):
            if not c.isalnum():
                raise ValueError("'name' must consist of only letters, numbers, and periods")
        if name in self._stats:
            s = self._stats[name]
            if not s._itype == itype:
                raise TypeError("%s already has type %s" % (name,s._itype.__name__))
        else:
            s = Stat(name, ivalue, itype, volatile)
            self._stats[name] = s
        return s

    def showStats(self, name, recursive=False):
        """
        """
        name = name.strip('.')
        if name != '':
            for c in name.split('.'):
                if not c.isalnum():
                    raise ValueError("'name' must consist of only letters, numbers, and periods")
        if recursive == False:
            try:
                return [(name, self._stats[name].value)]
            except:
                return []
        if name == '':
            return sorted([(k,v.value) for k,v in self._stats.items()])
        return sorted([(k,v.value) for k,v in self._stats.items()
            if k == name or k.startswith("%s." % name)])

    def _loadStats(self):
        """
        Load all stats from the statistics file.
        """
        if self.statsfile == None:
            return
        try:
            with open(self.statsfile, 'r') as f:
                for line in f.readlines():
                    try:
                        name,ivalue,value = line.split(' ', 2)
                        if ivalue.isdigit():
                            itype = long
                        else:
                            itype = float
                        stat = self.getStat(name, itype, itype(ivalue), False)
                        stat.value = itype(value)
                    except Exception, e:
                        logger.warning("failed to load statistic %s: %s" % (name,e))
        except (IOError,OSError), e:
            if e.errno != errno.ENOENT:
                logger.warning("failed to load statistics: %s" % e.strerror)
        except Exception, e:
            logger.warning("failed to load statistics: %s" % str(e))

    def _saveStats(self):
        """
        Write all statistics to the statistics file.
        """
        if self.statsfile == None or self._dirty == False:
            return
        try:
            with open(self.statsfile, 'w') as f:
                for name,s in sorted(self._stats.items(), lambda x,y: cmp(x[0],y[0])):
                    if s._volatile == False:
                        f.write("%s %s %s\n" % (name,s._ivalue,s._value))
            self._dirty = False
        except (IOError,OSError), e:
            logger.warning("failed to save statistics: %s" % e.strerror)
        except Exception, e:
            logger.warning("failed to save statistics: %s" % str(e))

    def flushStats(self, flushAll=False):
        """
        """
        pass


stats = StatsManager()
"""
"""

def getStat(name, ivalue):
    return stats.getStat(name, type(ivalue), ivalue, False)

def getVolatileStat(name, ivalue):
    return stats.getStat(name, type(ivalue), ivalue, True)
