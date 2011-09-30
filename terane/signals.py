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

from twisted.internet.defer import Deferred
from terane.loggers import getLogger

logger = getLogger('terane.signals')

class SignalDisconnected(Exception):
    pass

class Signal(object):

    def __init__(self):
        self._receivers = {}

    def matches(self, kwds):
        """
        Override this method in your signal subclass if you want to do keyword
        matching of signal receivers.  If you don't override this method, then
        all receivers will match when the signal is fired.
        """
        return True

    def connect(self, **kwds):
        """Connect to a signal.  Returns a deferred."""
        d = Deferred()
        d.kwds = kwds
        self._receivers[d] = d
        return d

    def _onDisconnect(self, failure):
        pass

    def disconnect(self, d):
        """Disconnect a receiver from a signal."""
        if not d in self._receivers:
            raise KeyError()
        del self.receivers[d]
        d.errback(SignalDisconnected())

    def signal(self, result):
        """Signal all registered receivers."""
        deferreds = self._receivers.values()
        self._receivers = {}
        for d in deferreds:
            if self.matches(d.kwds):
                logger.trace("signaling receiver %s" % d)
                d.callback(result)
