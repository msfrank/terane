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

import os, sys, signal, atexit
from logging import StreamHandler, FileHandler, Formatter
from twisted.internet import reactor
from twisted.application.service import MultiService
from twisted.internet.defer import maybeDeferred
from terane.plugins import plugins
from terane.routes import routes
from terane.queries import queries
from terane.idgen import idgen
from terane.stats import stats
from terane.loggers import getLogger, startLogging, StdoutHandler, FileHandler
from terane.loggers import ERROR, WARNING, INFO, DEBUG

logger = getLogger('terane.commands.server.server')

class ServerError(Exception):
    pass

class Server(MultiService):

    def configure(self, settings):
        self.settings = settings
        section = settings.section('server')
        self.pidfile = section.getPath('pid file', '/var/run/terane/server.pid')
        self.debug = section.getBoolean('debug', False)
        logconfigfile = section.getString('log config file', "%s.logconfig" % settings.appname)
        if section.getBoolean("debug", False):
            startLogging(StdoutHandler(), DEBUG, logconfigfile)
        else:
            logfile = section.getPath('log file', 'var/log/terane/server.log')
            verbosity = section.getString('log verbosity', 'WARNING')
            if verbosity == 'DEBUG': level = DEBUG
            elif verbosity == 'INFO': level = INFO
            elif verbosity == 'WARNING': level = WARNING
            elif verbosity == 'ERROR': level = ERROR
            else: raise ConfigureError("Unknown log verbosity '%s'" % verbosity)
            startLogging(FileHandler(logfile), level, logconfigfile)
        self.threadpoolsize = section.getInt('thread pool size', 20)
        reactor.suggestThreadPoolSize(self.threadpoolsize)

    def run(self):
        # check that the pid file doesn't exist
        try:
            fd = os.open(self.pidfile, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0644)
            os.close(fd)
        except OSError, e:
            if e.errno == 17:
                f = open(self.pidfile, 'r')
                pid = f.readline().rstrip()
                f.close()
                raise ServerError("terane-server is already running (pid %s)" % pid)
            else:
                raise ServerError("failed to create PID file %s: %s" % (self.pidfile,e.strerror))
        except Exception, e:
            raise ServerError("failed to create PID file %s: %s" % (self.pidfile,e))
        # if --debug was not specified
        if self.debug == False:
            # fork once to go into the background
            if os.fork() != 0:
                os._exit(0)
            # create a new session, which creates a new process group and
            # also guarantees that we have no controlling terminal
            os.setsid()
            # fork twice, making init responsible for cleanup
            if os.fork() != 0:
                os._exit(0)
            # close stdin, stdout, and stderr
            os.close(0)
            os.close(1)
            os.close(2)
            # redirect stdin, stdout, and stderr to /dev/null
            os.dup2(os.open('/dev/null', os.O_RDWR), 0)
            os.dup2(0, 1)
            os.dup2(0, 2)
            # chdir to the filesystem root so we don't prevent unmounting
            os.chdir('/')
        # write PID to the pidfile
        f = open(self.pidfile, 'w')
        f.write("%i\n" % os.getpid())
        f.close()
        atexit.register(self._removePid)
        # configure the statistics manager
        stats.setServiceParent(self)
        stats.configure(self.settings)
        # configure the plugin manager
        plugins.setServiceParent(self)
        plugins.configure(self.settings)
        # configure the route manager
        routes.setServiceParent(self)
        routes.configure(self.settings)
        # configure the query manager
        queries.setServiceParent(self)
        queries.configure(self.settings)
        # configure the id generator
        idgen.setServiceParent(self)
        idgen.configure(self.settings)
        # catch SIGINT and SIGTERM
        signal.signal(signal.SIGINT, self._signal)
        signal.signal(signal.SIGTERM, self._signal)
        # start all services
        self.startService()
        # Quaid, start the reactor!
        logger.debug("-------- starting terane server --------")
        reactor.run()
        # ignore SIGINT and SIGTERM, since we are shutting down
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        logger.debug("stopped terane server")
        return 0

    def _signal(self, signum, frame):
        print
        logger.debug("exiting on signal %i" % signum)
        self.stop()

    def stop(self):
        if not reactor.running or not self.running:
            raise Exception("server is not running")
        d = maybeDeferred(self.stopService)
        d.addCallback(self._stop)

    def _stop(self, result):
        reactor.stop()
        self._removePid()

    def _removePid(self):
        try:
            os.remove(self.pidfile)
        except:
            pass
