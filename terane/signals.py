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

from zope.interface import Interface
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.task import deferLater
from terane.loggers import getLogger

logger = getLogger('terane.signals')

class SignalCancelled(Exception):
    pass

class ICopyable(Interface):
    def copy():
        """
        Return a copy of the object.

        :returns: A copy of the object.
        :rtype: object
        """

class Signal(object):

    def __init__(self):
        self._receivers = set()

    def matches(self, kwds):
        """
        Override this method in your signal subclass if you want to do keyword
        matching of signal receivers.  If you don't override this method, then
        all receivers will match when the signal is fired.

        :param kwds:
        :type: dict:
        :returns: True if the receiver matches, otherwise False.
        :rtype: bool
        """
        return True

    def connect(self, **kwds):
        """
        Connect a receiver to the signal.
      
        :returns: A Deferred which fires when a signal is received.
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        d = Deferred()
        d.kwds = kwds
        self._receivers.add(d)
        return d

    def disconnect(self, d):
        """
        Disconnect a receiver from a signal.

        :param d: The connected receiver.
        :type d: :class:`twisted.internet.defer.Deferred`
        :raises KeyError: The specified receiver doesn't exist.
        """
        if not d in self._receivers:
            raise KeyError("signal does not contain the specified receiver")
        self._receivers.remove(d)
        d.errback(SignalCancelled())

    def signal(self, result):
        """
        Signal all registered receivers.

        :param result: The data to pass to receivers.
        :type result: object implementing :class:`terane.signals.ICopyable`
        :raises TypeError: result doesn't implement ICopyable.
        """
        if not ICopyable.providedBy(result):
            raise TypeError("result does not implement ICopyable")
        receivers = self._receivers
        self._receivers = set()
        for d in receivers:
            if self.matches(d.kwds):
                logger.trace("signaling receiver %s" % d)
                # return a copy of the result, so the receiver can modify it
                d.callback(result.copy())
