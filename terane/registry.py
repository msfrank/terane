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

import types
from zope.interface import Interface
from zope.interface.interface import InterfaceClass
from terane.loggers import getLogger

logger = getLogger('terane.registry')

class Registry(object):
    """
    Inversion of Control container.
    """

    def __init__(self):
        self._components = {}

    def addComponent(self, component, spec, name=None):
        """
        Add a component to the registry.  The component can be anything;
        it is an opaque object which depends on the registrar and retreiver
        to have previously agreed on its meaning.

        :param component: The component to register.
        :type component: object
        :param spec: The class or interface which the component provides.
        :type spec: type
        :param name: The implementation name.
        :type name: str
        """
        if not (isinstance(spec, types.ClassType) 
          or isinstance(spec, types.TypeType)
          or isinstance(spec, InterfaceClass)):
            raise TypeError("spec must be a class or Interface")
        if (spec,name) in self._components:
            raise KeyError("component %s:%s already exists" % (spec.__name__,name))
        self._components[(spec,name)] = component
        if name:
            logger.trace("added component %s:%s" % (spec.__name__, name))
        else:
            logger.trace("added component %s" % spec.__name__)

    def getComponent(self, spec, name=None):
        """
        Get a component from the registry.  The component can be anything;
        it is an opaque object which depends on the registrar and retreiver
        to have previously agreed on its meaning.

        :param spec: The class or interface which the component provides/implements.
        :type spec: type
        :param name: The implementation name.
        :type name: str
        :returns: The component.
        :rtype: object
        """
        if not (isinstance(spec, types.ClassType) 
          or isinstance(spec, types.TypeType)
          or isinstance(spec, InterfaceClass)):
            raise TypeError("spec must be a class or Interface")
        if not (spec,name) in self._components:
            raise KeyError("component %s:%s not found" % (spec.__name__,name))
        return self._components[(spec,name)]


_registry = Registry()

def getRegistry():
    """
    Return a reference to the global registry.

    :returns: The global registry.
    :rtype: :class:`terane.registry.Registry`
    """
    return _registry
