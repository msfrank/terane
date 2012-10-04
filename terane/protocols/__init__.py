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

from terane.plugins import ILoadable

class IProtocol(ILoadable):
    def getDefaultPort():
        """
        Return the port which the protocol suggests the listener should
        listen on.  The listener can override this by specifying a
        'listen port' configuration setting in the [listener] section.

        :returns: The suggested listening port.
        :rtype: int
        """
    def makeFactory():
        """

        :returns: A ServerFactory suitable to pass to listenTCP. 
        :rtype: :class:`twisted.internet.protocol.ServerFactory`
        """
            
class Protocol(object):

    def __init__(self, plugin):
        pass

    def getDefaultPort(self):
        return None

    def makeFactory(self):
        raise NotImplementedError()
