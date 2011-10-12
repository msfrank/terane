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

import os, traceback
from twisted.application.service import Service, MultiService, IServiceCollection
from zope.interface import Attribute
from pkg_resources import Environment, working_set
from terane.settings import ConfigureError
from terane.loggers import getLogger

logger = getLogger('terane.plugins')

class IPlugin(IServiceCollection):
    factory = Attribute("The factory for producing plugin instances.")
    def configure(section):
        "Configure the plugin."

class Plugin(MultiService):
    """
    Abstract class which all plugins inherit from.
    """

    factory = None

    def __init__(self):
        MultiService.__init__(self)

    def configure(self, section):
        """
        Subclasses should implement this method to parse configuration from
        the configuration file and command-line arguments.  If unimplemented,
        the default implementation does nothing.

        :param section: The configuration section.
        :type section: :class:`terane.settings.Section`
        """
        pass

    def startService(self):
        """
        Called when the plugin is started.  If a plugin needs to perform any
        startup tasks, they should override this method (be sure to chain up
        to the parent method) and perform them here.
        """
        MultiService.startService(self)

    def stopService(self):
        """
        Called when the plugin is stopped.  If a plugin needs to perform any
        shutdown task, they should override this method (be sure to chain up
        to the parent method) and perform them here.
        """
        return MultiService.stopService(self)

class PluginManager(MultiService):
    """
    Manages the lifecycle of all plugins.  Each plugin is loaded during the
    configure phase, and its configure method is called.  When terane enters
    the main loop, the startService method of each plugin is called.  When
    terane exits the main loop, the stopService method of each plugin is called.
    """

    def __init__(self):
        MultiService.__init__(self)
        self.setName("plugins")
        self._plugins = {}
        self._instances = {}

    def configure(self, settings):
        """
        Finds all discoverable plugins and configures them.  Plugins are
        discoverable if they are in the normal python module path, or in
        the path specified by 'plugin directory'.
        """
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
        for ptype in ['protocol','input','output','filter']:
            plugins = {}
            for ep in working_set.iter_entry_points("terane.plugin.%s" % ptype):
                if not settings.hasSection("plugin:%s:%s" % (ptype, ep.name)):
                    continue
                # if a configuration section exists, load and configure the plugin
                try:
                    _Plugin = ep.load()
                    if not IPlugin.implementedBy(_Plugin):
                        raise Exception("%s plugin '%s' doesn't implement IPlugin" % (ptype, ep.name))
                    section = settings.section("plugin:%s:%s" % (ptype, ep.name))
                    plugin = _Plugin()
                    plugin.setName("plugin:%s:%s" % (ptype, ep.name))
                    plugin.setServiceParent(self)
                    plugin.configure(section)
                    plugins[ep.name] = plugin
                    logger.info("loaded %s plugin '%s'" % (ptype, ep.name))
                except ConfigureError:
                    raise
                except Exception, e:
                    logger.exception(e)
                    logger.warning("failed to load %s plugin '%s'" % (ptype, ep.name))
            self._plugins[ptype] = plugins
        # instanciate all configured plugins for each type
        for ptype in ['protocol','input','output','filter']:
            instances = {}
            for section in settings.sectionsLike("%s:" % ptype):
                iname = section.name.split(':',1)[1]
                try:
                    itype = section.getString('type', None)
                    if itype == None:
                        raise ConfigureError("plugin instance missing required option 'type'")
                    plugins = self._plugins[ptype]
                    try:
                        plugin = plugins[itype]
                    except:
                        raise ConfigureError("no registered %s plugin named '%s'" % (ptype, itype))
                    instance = plugin.factory()
                    instance.setName(iname)
                    instance.setServiceParent(plugin)
                    instance.configure(section)
                    instances[iname] = instance
                except ConfigureError:
                    raise
                except Exception, e:
                    logger.exception(e)
                    logger.warning("failed to load %s instance '%s'" % (ptype, iname))
            self._instances[ptype] = instances
 
    def instance(self, ptype, iname):
        """
        Returns an instance of the specified plugin.
        
        :param ptype: The type of plugin.
        :type ptype: str
        :param iname: The name of the plugin instance.
        :type iname: str
        :returns: An instance of the specified plugin.
        :rtype: :class:`terane.plugins.Plugin`
        :raises Exception: The specified plugin was not found.
        """
        return self._instances[ptype][iname]

    def instancesImplementing(self, ptype, iface):
        """
        Returns a list of instances providing the specified interface.
        
        :param ptype: The type of plugin.
        :type ptype: str
        :param iface: The interface.
        :type iname: zope.interface.Interface
        :returns: A list of instances providing the specified interface.
        :rtype: list
        """
        return [i for i in self._instances[ptype].values() if iface.providedBy(i)]


plugins = PluginManager()
"""
`plugins` is a singleton instance of a :class:`PluginManager`.  All interaction
with the plugin infrastructure must occur through this instance; do *not* instantiate
new :class:`PluginManager` instances!
"""
