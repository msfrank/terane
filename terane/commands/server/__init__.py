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
from terane.commands.server.server import Server, ServerError
from terane.settings import Settings, ConfigureError

def server_main():
    try:
        settings = Settings(usage="%prog [options...]")
        settings.addSwitch("-d","--debug", ("server","debug"),
            help="do not fork into the background, log to stderr",
            )
        settings.addOption("-f","--logfile", ("server","log file"),
            help="log messages to FILE (--debug overrides this)", metavar="FILE"
            )
        settings.addOption("-p","--pidfile", ("server","pid file"),
            help="write PID to FILE", metavar="FILE"
            )
        # load settings from command line arguments, config file
        settings.load()
        # configure the server
        server = Server()
        server.configure(settings)
        # start the server
        status = server.run()
        return
    except (ConfigureError,ServerError), e:
        print e
    except Exception, e:
        print "\nUnhandled Exception:\n%s\n---\n%s" % (e,traceback.format_exc())
    sys.exit(1)
