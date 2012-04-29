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

import os, sys, traceback
from terane.commands.grok import commands
from terane.settings import Settings, ConfigureError

def grok_main():
    try:
        settings = Settings(usage="[OPTIONS...] COMMAND")
        # declare global options
        settings.addOption("H","host", "grok", "host",
            help="Connect to terane server HOST", metavar="HOST"
            )
        settings.addOption('',"log-config", "grok", "log config file",
            help="use logging configuration file FILE", metavar="FILE"
            )
        settings.addSwitch("d","debug", "grok", "debug",
            help="Print debugging information"
            )
        # declare list-indices command
        settings.addSubcommand("list-indices", usage="",
            description="List available indices", handler=commands.ListIndices
            )
        # declare show-index command
        settings.addSubcommand("show-index", usage="[OPTIONS...] INDEX",
            description="Show index statistics", handler=commands.ShowIndex
            )
        # declare show-stats command
        subcommand = settings.addSubcommand("show-stats", usage="[OPTIONS...] STAT",
            description="Display server statistics", handler=commands.ShowStats
            )
        subcommand.addSwitch("r", "recursive", "show-stats", "recursive",
            help="Recursively display all stats"
            )
        # declare flush-stats command
        subcommand = settings.addSubcommand("flush-stats", usage="[OPTIONS...]",
            description="Reset server statistics", handler=commands.FlushStats
            )
        subcommand.addSwitch("a", "all", "flush-stats", "flush all",
            help="Reset persistent statistics as well"
            )
        # load configuration
        settings.load()
        # create the Searcher and run it
        handler = settings.getHandler()
        command = handler()
        command.configure(settings)
        return command.run()
    except ConfigureError, e:
        print e
    except Exception, e:
        print "\nUnhandled Exception:\n%s\n---\n%s" % (e,traceback.format_exc())
    sys.exit(1)
