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
from os.path import normpath, join
from ConfigParser import SafeConfigParser
from optparse import OptionParser, OptionError
from terane.loggers import getLogger
from terane import versionstring

logger = getLogger('terane.settings')

class ConfigureError(Exception):
    """
    Configuration parsing failed.
    """
    pass

class Settings(object):
    """
    Contains configuration loaded from the configuration file and
    parsed from command-line arguments.

    :param usage: The usage string, displayed in --help output
    :type usage: str
    :param description: A short application description, displayed in --help output
    :type description: str
    """

    def __init__(self, usage=None, description=None):
        self._parser = OptionParser(version="%prog " + versionstring(),
            usage=usage, description=description)
        self._parser.add_option("-c","--config-file",
            dest="config_file",
            help="use global configuration file FILE",
            metavar="FILE",
            default="/etc/terane/terane.conf"
            )
        self._config = SafeConfigParser()
        self._overrides = SafeConfigParser()
        self._cwd = os.getcwd()

    def _optionCallback(self, option, opt, value, parser, override):
        if not self._overrides.has_section(override[0]):
            self._overrides.add_section(override[0])
        self._overrides.set(override[0], override[1], value)

    def addOption(self, shortname, longname, override, help=None, metavar=None):
        """
        Add a command-line option to be parsed.  An option (as opposed to a switch)
        is required to have an argument.

        :param shortname: the one letter option name.
        :type shortname: str
        :param longname: the long option name.
        :type longname: str
        :param override: A tuple specifying the section:name.
        :type override: (str,str)
        :param help: The help string, displayed in --help output.
        :type help: str
        :param metavar: The variable displayed in the help string
        :type metavar: str
        """
        self._parser.add_option(shortname, longname,
            help=help, metavar=metavar, type=str,
            action="callback", callback=self._optionCallback, callback_args=(override,)
            )

    def _switchCallback(self, option, opt, value, parser, override, reverse):
        if not self._overrides.has_section(override[0]):
            self._overrides.add_section(override[0])
        if reverse:
            self._overrides.set(override[0], override[1], "false")
        else:
            self._overrides.set(override[0], override[1], "true")

    def addSwitch(self, shortname, longname, override, reverse=False, help=None):
        """
        Add a command-line switch to be parsed.  A switch (as opposed to an option)
        has no argument.

        :param shortname: the one letter option name.
        :type shortname: str
        :param longname: the long option name.
        :type longname: str
        :param override: A tuple specifying the section:name.
        :type override: (str,str)
        :param reverse: If True, then the meaning of the switch is reversed.
        :type reverse: bool
        :param help: The help string, displayed in --help output.
        :type help: str
        """
        self._parser.add_option(shortname, longname,
            help=help, type=None,
            action="callback", callback=self._switchCallback, callback_args=(override,reverse)
            )

    def load(self):
        """
        Load configuration from the configuration file and from command-line arguments.
        """
        # parse command line arguments
        opts,self._args = self._parser.parse_args()
        try:
            # load configuration file
            path = normpath(join(self._cwd, opts.config_file))
            with open(path, 'r') as f:
                self._config.readfp(f, path)
            logger.debug("loaded settings from %s" % path)
        except EnvironmentError, e:
            raise ConfigureError("failed to read configuration %s: %s" % (path,e.strerror))
        # merge command line settings with config file settings
        for section in self._overrides.sections():
            for name,value in self._overrides.items(section):
                if not self._config.has_section(section):
                    self._config.add_section(section)
                self._config.set(section, name, str(value))

    def args(self):
        """
        Get the list of non-option arguments passed on the command line.

        :returns: A list of argument strings.
        :rtype: [str]
        """
        return list(self._args)

    def hasSection(self, name):
        """
        Returns True if the specified section exists, otherwise False.

        :param name: The section name.
        :type name: str
        :returns: True or False.
        :rtype: [bool]
        """
        return self._config.has_section(name)

    def section(self, name):
        """
        Get the section with the specified name.  Note if the section
        does not exist, this method still doesn't fail.

        :param name: The section name.
        :type name: str
        :returns: The specified section.
        :rtype: :class:`Section`
        """
        return Section(name, self)

    def sections(self):
        """
        Return a list of all sections.

        :returns: A list of all sections.
        :rtype: :[class:`Section`]
        """
        sections = []
        for name in self._config.sections():
            sections.append(Section(name, self))
        return sections

    def sectionsLike(self, startsWith):
        """
        Return a list of all sections which start with the specified prefix.

        :param startsWith: The section name prefix.
        :type name: str
        :returns: A list of matching sections.
        :rtype: [:class:`Section`]
        """
        sections = []
        for name in [s for s in self._config.sections() if s.startswith(startsWith)]:
            sections.append(Section(name, self))
        return sections

class Section(object):
    """
    A group of configuration values which share a common purpose.

    :param name: The name of the section.
    :type name: str
    :param settings: The parent :class:`Settings` instance.
    :type settings: :class:`terane.settings.Settings`
    """

    def __init__(self, name, settings):
        self.name = name
        self._settings = settings

    def getString(self, name, default=None):
        """
        Returns the configuration value associated with the specified name,
        coerced into a str.  If there is no configuration value in the section
        called `name`, then return the value specified by `default`.  Note that
        `default` is returned unmodified (i.e. not coerced into a string).
        This makes it easy to detect if a configuration value is not present
        by setting `default` to None.

        :param name: The configuration setting name.
        :type name: str
        :param default: The value to return if a value is not found.
        :returns: The string value, or the default value.
        """
        if not self._settings._config.has_option(self.name, name):
            return default
        return self._settings._config.get(self.name, name).strip()

    def getInt(self, name, default=None):
        """
        Returns the configuration value associated with the specified name,
        coerced into a int.  If there is no configuration value in the section
        called `name`, then return the value specified by `default`.  Note that
        `default` is returned unmodified (i.e. not coerced into an int).
        This makes it easy to detect if a configuration value is not present
        by setting `default` to None.

        :param name: The configuration setting name.
        :type name: str
        :param default: The value to return if a value is not found.
        :returns: The int value, or the default value.
        """
        if not self._settings._config.has_option(self.name, name):
            return default
        return self._settings._config.getint(self.name, name)

    def getBoolean(self, name, default=None):
        """
        Returns the configuration value associated with the specified name,
        coerced into a bool.  If there is no configuration value in the section
        called `name`, then return the value specified by `default`.  Note that
        `default` is returned unmodified (i.e. not coerced into a bool).
        This makes it easy to detect if a configuration value is not present
        by setting `default` to None.

        :param name: The configuration setting name.
        :type name: str
        :param default: The value to return if a value is not found.
        :returns: The bool value, or the default value.
        """
        if not self._settings._config.has_option(self.name, name):
            return default
        return self._settings._config.getboolean(self.name, name)

    def getFloat(self, name, default=None):
        """
        Returns the configuration value associated with the specified name,
        coerced into a float.  If there is no configuration value in the section
        called `name`, then return the value specified by `default`.  Note that
        `default` is returned unmodified (i.e. not coerced into a float).
        This makes it easy to detect if a configuration value is not present
        by setting `default` to None.

        :param name: The configuration setting name.
        :type name: str
        :param default: The value to return if a value is not found.
        :returns: The float value, or the default value.
        """
        if not self._settings._config.has_option(self.name, name):
            return default
        return self._settings._config.getfloat(self.name, name)

    def getPath(self, name, default=None):
        """
        Returns the configuration value associated with the specified name,
        coerced into a str and normalized as a filesystem absolute path.  If
        there is no configuration value in the section called `name`, then
        return the value specified by `default`.  Note that `default` is
        returned unmodified (i.e. not coerced into a string).  This makes it
        easy to detect if a configuration value is not present by setting
        `default` to None.

        :param name: The configuration setting name.
        :type name: str
        :param default: The value to return if a value is not found.
        :returns: The string value, or the default value.
        """
        if not self._settings._config.has_option(self.name, name):
            return default
        path = self._settings._config.get(self.name, name)
        return normpath(join(self._settings._cwd, path))

    def getList(self, etype, name, default=None, delimiter=','):
        """
        Returns the configuration value associated with the specified `name`,
        coerced into a list of values with the specified `type`.  If
        there is no configuration value in the section called `name`, then
        return the value specified by `default`.  Note that `default` is
        returned unmodified (i.e. not coerced into a list).  This makes it
        easy to detect if a configuration value is not present by setting
        `default` to None.

        :param etype: The type of each element in the list.
        :type name: classtype
        :param name: The configuration setting name.
        :type name: str
        :param default: The value to return if a value is not found.
        :param delimiter: The delimiter which separates values in the list.
        :type delimiter: str
        :returns: The string value, or the default value.
        """
        if not self._settings._config.has_option(self.name, name):
            return default
        l = self._settings._config.get(self.name, name)
        try:
            return [etype(e.strip()) for e in l.split(delimiter)]
        except Exception, e:
            raise ConfigureError("failed to parse configuration item [%s]=>%s: %s" % (
                self.name, name, e))
