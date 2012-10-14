# Copyright 2010,2011,2012 Michael Frank <msfrank@syntaxjockey.com>
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

import os, datetime
from dateutil.tz import tzutc
from zope.interface import implements
from terane.manager import IManager, Manager
from terane.settings import ConfigureError
from terane.bier.interfaces import *
from terane.bier.event import Event
from terane.loggers import getLogger

logger = getLogger('terane.bier')

class EventManager(Manager):
    """
    """

    implements(IManager, IEventFactory, IFieldStore)

    def __init__(self, pluginstore):
        Manager.__init__(self)
        self.setName("events")
        self._pluginstore = pluginstore
        self._fields = dict()
        self._idstore = None
        self._idcache = []

    def configure(self, settings):
        """
        Configure the ID generator.
        """
        section = settings.section('server')
        self._backingfile = section.getString('id cache file', '/var/lib/terane/idgen')
        self.cachesize = section.getInt('id cache size', 256)
        if self.cachesize < 0:
            raise ConfigureError("'id cache size' cannot be smaller than 0")

    def startService(self):
        """
        Read the last document identifier from the backing file, and
        fill the cache with new identifiers.
        """
        Manager.startService(self)
        self._idstore = os.open(self._backingfile, os.O_RDWR | os.O_CREAT, 0600)
        self._refillcache()
        logger.debug("loaded %i entries into id cache" % self.cachesize)

    def stopService(self):
        """
        Write back the last document identifier, and sync the backing
        file to disk.
        """
        self._writelast(self._idcache.pop(0))
        os.close(self._idstore)
        self._idstore = None
        self._idcache = []
        return Manager.stopService(self)

    def getField(self, name):
        """
        Returns a singleton instance of the specified field.

        :returns: The field instance.
        :rtype: An object implementing :class:`terane.bier.IField`
        """
        if name in self._fields:
            return self._fields[name]
        factory = self._pluginstore.getComponent(IField, name)
        field = factory()
        self._fields[name] = field
        return field

    def makeEvent(self):
        """
        Returns a new Event with a unique event identifier.

        :returns: The new event.
        :rtype: :class:`terane.bier.event.Event`
        """
        return Event(datetime.datetime.now(tzutc()), self._allocateOffset())

    def _allocateOffset(self):
        """
        Return a new 64-bit long document identifier.
        """
        try:
            # try to use the cache first
            return long(self._idcache.pop(0))
        except IndexError:
            # if the cache is empty, the fill it back up
            self._refillcache()
            return long(self._idcache.pop(0))

    def _refillcache(self):
        """
        Generate a new cache of document identifiers.
        """
        if len(self._idcache) > 0:
            raise Exception("ID generator failed to refill cache: cache not empty")
        last = self._readlast()
        if last == 0:
            self._idcache = range(1, 1 + self.cachesize)
            self._writelast(1 + self.cachesize)
        else:
            self._idcache = range(last, last + self.cachesize)
            self._writelast(last + self.cachesize)

    def _readlast(self):
        """
        Read the last document identifier stored in the backing file.
        """
        try:
            # seek to the beginning of the file
            os.lseek(self._idstore, 0, os.SEEK_SET)
            # read 16 bytes from idgen
            data = os.read(self._idstore, 16)
        except EnvironmentError, (errno, strerror):
            raise Exception("ID generator failed to read from idgen: %s (%i)" % (strerror,errno))
        # indicates EOF.  this usually means idgen was just created, so return 0
        if data == '':
            return long(0)
        # if data isn't empty, then there should be 16 bytes of data to read
        if len(data) != 16:
            raise Exception("ID generator is corrupt: idgen data is too small")
        last = long(data, 16)
        # the id should be greater than 0
        if last < 1:
            raise Exception("ID generator is corrupt: last id is smaller than 1")
        return last

    def _writelast(self, last):
        """
        Write the last document identifier to in the backing file.
        """
        try:
            # seek to the beginning of the file
            os.lseek(self._idstore, 0, os.SEEK_SET)
            # read 16 bytes from idgen
            os.write(self._idstore, "%016x" % last)
            # sync any buffered data to disk
            os.fsync(self._idstore)
        except EnvironmentError, (errno, strerror):
            raise Exception("ID generator failed to write to idgen: %s (%i)" % (strerror,errno))
