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

import os, sys, getopt
from ConfigParser import RawConfigParser
from terane.loggers import getLogger
from terane import versionstring

logger = getLogger('terane.settings')

class ConfigureError(Exception):
    """
    Configuration parsing failed.
    """
    pass

class Parser(object):
    def __init__(self, name, usage, description, parent=None, handler=None):
        self.name = name
        self.usage = usage
        self.description = description
        self._parent = parent
        self._handler = handler
        self._subcommands = {}
        self._options = {}
        self._shortnames = ''
        self._longnames = ['help']

    def addSubcommand(self, name, usage, description, handler=None):
        if name in self._subcommands:
            raise ConfigureError("subcommand '%s' is already defined" % name)
        subcommand = Parser(name, usage, description, self, handler)
        self._subcommands[name] = subcommand
        return subcommand

    def addOption(self, shortname, longname, section, override, help=None, metavar=None):
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
        if shortname in self._options:
            raise ConfigureError("-%s is already defined" % shortname)
        if longname in self._options:
            raise ConfigureError("--%s is already defined" % longname)
        o = dict(type='option', shortname=shortname, longname=longname,
                 section=section, override=override, help=help, metavar=metavar)
        self._options[shortname] = o
        self._shortnames += "%s:" % shortname
        self._options[longname] = o
        self._longnames.append("%s=" % longname)

    def addSwitch(self, shortname, longname, section, override, reverse=False, help=None):
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
        if shortname in self._options:
            raise ConfigureError("-%s is already defined" % shortname)
        if longname in self._options:
            raise ConfigureError("--%s is already defined" % longname)
        s = dict(type='switch', shortname=shortname, longname=longname,
                 section=section, override=override, reverse=reverse, help=help)
        self._options[shortname] = s
        self._shortnames += shortname
        self._options[longname] = s
        self._longnames.append(longname)

    def _parse(self, argv, store):
        """
        Parse the command line specified by argv, and store the options
        in store.
        """
        if len(self._subcommands) == 0:
            opts,args = getopt.gnu_getopt(argv, self._shortnames, self._longnames)
        else:
            opts,args = getopt.getopt(argv, self._shortnames, self._longnames)
        for opt,value in opts:
            if opt == '--help': self._usage()
            o = self._options[opt]
            if not store.has_section(o['section']):
                store.add_section(o['section'])
            if o['type'] == 'switch':
                if o['reverse'] == True:
                    store.set(o['section'], o['override'], 'false')
                else:
                    store.set(o['section'], o['override'], 'true')
            else:
                store.set(o['section'], o['override'], value)
        if len(self._subcommands) > 0:
            if len(args) == 0:
                raise ConfigureError("no subcommand specified")
            if not args[0] in self._subcommands:
                raise ConfigureError("no subcommand named '%s'" % args[0])
            return self._subcommands[args[0]]._parse(args[1:], store)
        return self, args

    def _usage(self):
        """
        Display a usage message and exit.
        """
        commands = []
        c = self
        while c != None:
            commands = [c.name] + commands
            c = c._parent
        print "Usage: %s %s" % (' '.join(commands), self.usage)
        print 
        # display the description, if it was specified
        if self.description != None and self.description != '':
            print self.description
            print
        # display options
        # display subcommands, if there are any
        if len(self._subcommands) > 0:
            print "Available Sub-commands:"
            print
            for name,parser in sorted(self._subcommands.items()):
                print " %s" % name
            print
        sys.exit(0)

class Settings(Parser):
    """
    Contains configuration loaded from the configuration file and
    parsed from command-line arguments.
    """

    def __init__(self, usage='', description=''):
        """
        :param usage: The usage string, displayed in --help output
        :type usage: str
        :param description: A short application description, displayed in --help output
        :type description: str
        """
        self.appname = os.path.basename(sys.argv[0])
        Parser.__init__(self, self.appname, usage, description)
        self._config = RawConfigParser()
        self._overrides = RawConfigParser()
        self._cwd = os.getcwd()
        self._overrides.add_section('settings')
        self._overrides.set('settings', 'config file', "/etc/terane/%s.conf" % self.appname)
        self.addOption('c', 'config-file', 'settings', 'config file',
            help="Load configuration from FILE", metavar="FILE")

    def load(self, needsconfig=False):
        """
        Load configuration from the configuration file and from command-line arguments.

        :param needsconfig: True if the config file must be present for the application to function.
        :type needsconfig: bool
        """
        # parse command line arguments
        self._parser,self._args = self._parse(sys.argv[1:], self._overrides)
        try:
            # load configuration file
            config_file = self._overrides.get('settings', 'config file')
            path = os.path.normpath(os.path.join(self._cwd, config_file))
            with open(path, 'r') as f:
                self._config.readfp(f, path)
            logger.debug("loaded settings from %s" % path)
        except EnvironmentError, e:
            if needsconfig:
                raise ConfigureError("failed to read configuration %s: %s" % (path,e.strerror))
            logger.info("didn't load configuration %s: %s" % (path,e.strerror))
        # merge command line settings with config file settings
        for section in self._overrides.sections():
            for name,value in self._overrides.items(section):
                if not self._config.has_section(section):
                    self._config.add_section(section)
                self._config.set(section, name, str(value))

    def getHandler(self):
        """
        """
        return self._parser._handler

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
        return os.path.normpath(os.path.join(self._settings._cwd, path))

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
