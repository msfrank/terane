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
from pkg_resources import Environment, working_set
from zope.interface import Interface, implements
from twisted.application.service import IService, Service
from terane import IManager, Manager
from terane.registry import getRegistry
from terane.settings import ConfigureError
from terane.loggers import getLogger

logger = getLogger('terane.plugins')

class IPlugin(IService):
    def configure(section):
        """
        Configure the plugin.

        :param section: The configuration section.
        :type section: :class:`terane.settings.Section`
        """
    def listComponents():
        """
        Get all of the components which the plugin advertises.

        :returns: A list of (impl,spec,name) tuples.
        :rtype: list
        """

class ILoadable(Interface):
    def __call__(plugin):
        """
        Pass the parent plugin instance to the constructor.

        :param plugin: The plugin parent.
        :type plugin: :class:`terane.plugins.Plugin`
        """

class Plugin(Service):
    """
    Abstract class which all plugins inherit from.
    """

    components = None

    def __init__(self):
        pass

    def configure(self, section):
        """
        Subclasses should implement this method to parse configuration from
        the configuration file and command-line arguments.  If unimplemented,
        the default implementation does nothing.

        :param section: The configuration section.
        :type section: :class:`terane.settings.Section`
        """
        pass

    def listComponents(self):
        """
        returns the list of components specified in the components class
        attribute.
        """
        if not self.components:
            raise NotImplementedError()
        return self.components

    def startService(self):
        """
        Called when the plugin is started.  If a plugin needs to perform any
        startup tasks, they should override this method (be sure to chain up
        to the parent method) and perform them here.
        """
        Service.startService(self)

    def stopService(self):
        """
        Called when the plugin is stopped.  If a plugin needs to perform any
        shutdown task, they should override this method (be sure to chain up
        to the parent method) and perform them here.
        """
        return Service.stopService(self)

class PluginManager(Manager):
    """
    Manages the lifecycle of all plugins.  Each plugin is loaded during the
    configure phase, and its configure method is called.  When terane enters
    the main loop, the startService method of each plugin is called.  When
    terane exits the main loop, the stopService method of each plugin is called.
    """

    implements(IManager)

    def __init__(self):
        Manager.__init__(self)
        self.setName("plugins")

    def configure(self, settings):
        """
        Finds all discoverable plugins and configures them.  Plugins are
        discoverable if they are in the normal python module path, or in
        the path specified by 'plugin directory'.
        """
        registry = getRegistry()
        # load plugins
        section = settings.section('server')
        self.pluginsdir = os.path.join(section.getPath("plugin directory"))
        if self.pluginsdir:
            logger.debug("loading plugins from %s" % self.pluginsdir)
            working_set.add_entry(self.pluginsdir)
            env = Environment([self.pluginsdir,])
        else:
            env = Environment([])
        self._eggs,errors = working_set.find_plugins(env)
        # load plugin eggs
        for p in self._eggs:
            working_set.add(p)
            logger.info("loaded plugin egg '%s'" % p)
        for e in errors:
            logger.info("failed to load plugin egg '%s'" % e)
        # load all discovered plugins for each type
        for ep in working_set.iter_entry_points("terane.plugin"):
            # if no config section exists, then don't load the plugin
            if not settings.hasSection("plugin:%s" % ep.name):
                continue
            try:
                # load and configure the plugin
                _Plugin = ep.load()
                if not IPlugin.implementedBy(_Plugin):
                    raise Exception("plugin '%s' doesn't implement IPlugin" % ep.name)
                plugin = _Plugin()
                plugin.setName(ep.name)
                plugin.setServiceParent(self)
                section = settings.section("plugin:%s" % ep.name)
                plugin.configure(section)
                registry.addComponent(plugin, IPlugin, ep.name)
                logger.info("loaded plugin '%s'" % ep.name)
                # find all plugin components and load them into the registry
                for impl,spec,name in plugin.listComponents():
                    if not ILoadable.implementedBy(impl):
                        raise Exception("component %s:%s in plugin %s doesn't implement ILoadable" % 
                            (spec.__name__, name, ep.name))
                    # a little extra syntax here to make sure the lambda expression
                    # passed as the factory function to addComponent() has the appropriate
                    # variables bound in its scope
                    def _makeTrampoline(impl=impl, plugin=plugin):
                        def _trampoline():
                            logger.trace("allocating new %s from plugin %s" % (impl.__name__,plugin.name))
                            return impl(plugin)
                        return _trampoline
                    registry.addComponent(_makeTrampoline(impl, plugin), spec, name)
            except ConfigureError:
                raise
            except Exception, e:
                logger.exception(e)
                logger.warning("failed to load plugin '%s'" % ep.name)
