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

from twisted.application.service import IService, Service
from zope.interface import Interface
from terane.plugins import ILoadable

class IOutput(IService, ILoadable):
    def configure(section):
        "Configure the plugin instance."
    def getContract():
         "Return a set of field names which the receiveEvent method expects."
    def receiveEvent(fields):
        "Receive a dict of event fields and store them."

class ISearchable(Interface):
    def getIndex():
        "Return the Index."

class Output(Service):
    """
    The Output base implementation.
    """

    def __init__(self, plugin):
        pass

    def configure(self, section):
        pass

    def startService(self):
        Service.startService(self)

    def stopService(self):
        Service.stopService(self)

    def receiveEvent(self, event):
        pass
