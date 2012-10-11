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

from twisted.application.service import IService, Service
from terane.plugins import ILoadable
from terane.signals import Signal
from terane.bier.event import Event

class IInput(IService, ILoadable):
    def configure(section):
        "Configure the input."
    def getContract():
        "Return a Contract describing the fields which the Input emits."
    def getDispatcher():
        "Return an Dispatcher which the input uses to signal new events."

class Input(Service):
    """
    The Input base implementation.
    """

    def __init__(self, plugin, name, eventfactory):
        self.plugin = plugin
        self.name = name

    def configure(self, section):
        pass

    def startService(self):
        Service.startService(self)

    def stopService(self):
        return Service.stopService(self)
