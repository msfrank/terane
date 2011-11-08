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
from terane.commands.console.console import console
from terane.settings import Settings, ConfigureError

def console_main():
    try:
        settings = Settings(usage="%prog [options...] [query]")
        settings.addOption("-H","--host", ("console","host"),
            help="Connect to terane server HOST", metavar="HOST"
            )
        settings.addOption("-e","--execute", ("console","execute command"),
            help="Execute CMD after startup", metavar="CMD"
            )
        settings.addOption('',"--log-config", ("console","log config file"),
            help="use logging configuration file FILE", metavar="FILE"
            )
        settings.addSwitch("-d","--debug", ("console","debug"),
            help="Print debugging information"
            )
        settings.load()
        console.configure(settings)
        return console.run()
    except ConfigureError, e:
        print e
    except Exception, e:
        print "\nUnhandled Exception:\n%s\n---\n%s" % (e,traceback.format_exc())
    sys.exit(1)
