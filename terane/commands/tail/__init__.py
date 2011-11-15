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
from terane.commands.tail.tailer import Tailer
from terane.settings import Settings, ConfigureError
from terane import versionstring

def tail_main():
    try:
        settings = Settings(usage="usage: %prog [options] query")
        settings.addOption("-H","--host", ("tail","host"),
            help="Connect to terane server HOST"
            )
        settings.addOption("-i","--use-indices", ("tail","use indices"),
            help="Search only the specified INDICES (comma-separated)", metavar="INDICES"
            )
        settings.addSwitch("-v","--verbose", ("tail", "long format"),
            help="Display more information about each event"
            )
        settings.addOption("-R","--refresh", ("tail", "refresh"),
            help="Request new data every INTERVAL seconds"
            )
        settings.addOption("-l","--limit", ("tail","limit"),
            help="Display the first LIMIT results", metavar="LIMIT"
            )
        settings.addOption("-f","--fields", ("tail","display fields"),
            help="Display only the specified FIELDS (comma-separated)", metavar="FIELDS"
            )
        settings.addOption("-t","--timezone", ("tail","timezone"),
            help="Convert timestamps to specified timezone", metavar="TZ"
            )
        settings.addOption('',"--log-config", ("tail","log config file"),
            help="use logging configuration file FILE", metavar="FILE"
            )
        settings.addSwitch("-d","--debug", ("tail", "debug"),
            help="Print debugging information"
            )
        # load configuration
        settings.load()
        # create the Tailer and run it
        tailer = Tailer()
        tailer.configure(settings)
        return tailer.run()
    except ConfigureError, e:
        print e
    except Exception, e:
        print "\nUnhandled Exception:\n%s\n---\n%s" % (e,traceback.format_exc())
    sys.exit(1)
