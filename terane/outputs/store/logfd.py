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
from twisted.internet.abstract import FileDescriptor
from terane.outputs.store import backend
from terane.loggers import getLogger

class LogFD(FileDescriptor):
    def __init__(self):
        self._fd = backend.log_fd()
        self._buffer = []
        FileDescriptor.__init__(self)

    def doRead(self):
        lines = os.read(self._fd, 8192).split('\n')
        if len(lines) == 1:
            self._buffer.append(lines[0])
        else:
            self._buffer.append(lines.pop(0))
            self._doLog()
            for i in range(0, len(lines) - 1):
                self._buffer.append(lines.pop(0))
                self._doLog()
            self._buffer.append(lines.pop(0))

    def _doLog(self):
        line = ''.join(self._buffer)
        self._buffer = []
        level, name, message = line.split(' ', 2)
        logger = getLogger(name)
        logger.msg(int(level), message)

    def fileno(self):
        return self._fd
