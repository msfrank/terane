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

import os, time
from twisted.application.service import Service
from terane.settings import ConfigureError
from terane.loggers import getLogger

logger = getLogger('terane.outputs.store.idgen')

class IDGenerator(Service):
    """
    The ID generator service hands out new document identifiers.
    """
    def __init__(self):
        self.setName('idgen')
        self._fd = None
        self._cache = []

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
        logger.debug("started id generator")
        self._fd = os.open(self._backingfile, os.O_RDWR | os.O_CREAT, 0600)
        self._refillcache()
        logger.debug("loaded %i entries into id cache" % self.cachesize)

    def stopService(self):
        """
        Write back the last document identifier, and sync the backing
        file to disk.
        """
        self._writelast(self._cache.pop(0))
        os.close(self._fd)
        self._fd = None
        self._cache = []
        logger.debug("stopped id generator")

    def allocate(self):
        """
        """
        return self._allocateOffset()

    def _allocateOffset(self):
        """
        Return a new 64-bit long document identifier.
        """
        try:
            # try to use the cache first
            return long(self._cache.pop(0))
        except IndexError:
            # if the cache is empty, the fill it back up
            self._refillcache()
            return long(self._cache.pop(0))

    def _refillcache(self):
        """
        Generate a new cache of document identifiers.
        """
        if len(self._cache) > 0:
            raise Exception("ID generator failed to refill cache: cache not empty")
        last = self._readlast()
        if last == 0:
            self._cache = range(1, 1 + self.cachesize)
            self._writelast(1 + self.cachesize)
        else:
            self._cache = range(last, last + self.cachesize)
            self._writelast(last + self.cachesize)

    def _readlast(self):
        """
        Read the last document identifier stored in the backing file.
        """
        try:
            # seek to the beginning of the file
            os.lseek(self._fd, 0, os.SEEK_SET)
            # read 16 bytes from idgen
            data = os.read(self._fd, 16)
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
            os.lseek(self._fd, 0, os.SEEK_SET)
            # read 16 bytes from idgen
            os.write(self._fd, "%016x" % last)
            # sync any buffered data to disk
            os.fsync(self._fd)
        except EnvironmentError, (errno, strerror):
            raise Exception("ID generator failed to write to idgen: %s (%i)" % (strerror,errno))


idgen = IDGenerator()
