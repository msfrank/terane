#Copyright 2010,2011 Michael Frank <msfrank@syntaxjockey.com>
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

import socket, datetime
from twisted.application.service import MultiService
from terane.plugins import plugins
from terane.filters import FilterError, StopFiltering
from terane.signals import SignalCancelled
from terane.settings import ConfigureError
from terane.loggers import getLogger

logger = getLogger('terane.routes')

class RoutingError(Exception):
    pass

class Route(object):

    def __init__(self, name):
        self.name = name

    def configure(self, section):
        # load the route input
        name = section.getString('input')
        try:
            self._input = plugins.instance('input', name)
        except:
            raise ConfigureError("no such input %s" % name)
        # load the route output
        name = section.getString('output')
        try:
            self._output = plugins.instance('output', name)
        except:
            raise ConfigureError("no such output %s" % name)
        # load the route filter chain
        self._filters = []
        filters = section.getString('filter', '').strip()
        filters = [f.strip() for f in filters.split('|') if not f == '']
        if len(filters) > 0:
            # verify each referenced filter has been loaded
            for name in filters:
                try:
                    instance = plugins.instance('filter', name)
                except:
                    raise ConfigureError("no such filter %s" % name)
                # add filter to the filter chain
                self._filters.append(instance)
            # verify that the filtering chain will work
            requiredfields = self._filters[0].outfields()
            for filter in self._filters[1:]:
                # verify that the every field the filter requires for input is available
                if not filter.infields().issubset(requiredfields):
                    raise ConfigureError("filtering chain schemas are not compatible")
                # update requiredfields
                requiredfields.update(filter.outfields())
            # warn if the input may not provide all required fields
            if not self._filters[0].infields().issubset(self._input.outfields()):
                logger.warn("[route:%s] input %s does not guarantee fields required by filter %s" %
                    (self.name, self._input.name, self._filters[0].name))
            # warn if the output may not receive all required fields
            if not self._output.infields().issubset(requiredfields):
                logger.warn("[route:%s] filter chain does not guarantee fields required by output %s" %
                    (self.name, self._output.name))
            logger.debug("[route:%s] route configuration: %s -> %s -> %s" %
                (self.name,self._input.name,' -> '.join(filters),self._output.name))
        else:
            logger.debug("[route:%s] route configuration: %s -> %s" %
                (self.name,self._input.name,self._output.name))
        # schedule the on_received_event signal
        self._scheduleReceivedEvent()

    def _scheduleReceivedEvent(self):
        self.d = self._input.on_received_event.connect()
        self.d.addCallback(self._receivedEvent)
        self.d.addErrback(self._errorReceivingEvent)

    def _receivedEvent(self, fields):
        try:
            # set default values for input, tx, hostname, and default fields
            if not 'input' in fields:
                fields['input'] = self._input.name
            if not 'ts' in fields:
                fields['ts'] = datetime.datetime.now().isoformat()
            if not 'hostname' in fields:
                fields['hostname'] = socket.gethostname()
            if not 'default' in fields:
                fields['default'] = ''
            # copy the fields dict, so we can directly modify the copy
            fields = fields.copy()
            # run the fields through the filter chain
            for filter in self._filters:
                fields = filter.filter(fields)
            # recheck that the required special fields are present
            requiredfields = set(('input','ts','hostname','default'))
            if not requiredfields.issubset(set(fields.keys())):
                raise FilterError("missing required fields: %s" %
                    ', '.join(set(fields.keys()).difference(requiredfields)))
            logger.debug("[route:%s] processed event" % self.name)
            # send the event to the output
            self._output.receiveEvent(fields)
        except StopFiltering, e:
            # the event was intentionally dropped during filtering
            logger.debug("[route:%s] dropped event: %s" % (self.name,e))
        # reschedule the on_received_event signal
        self._scheduleReceivedEvent()

    def _errorReceivingEvent(self, failure):
        if failure.check(SignalCancelled) == None:
            logger.debug("[route:%s] error processing event: %s" % (self.name,str(failure)))
            self._scheduleReceivedEvent()

    def close(self):
        if self.d != None:
            self._input.on_received_event.disconnect(self.d)
            self.d = None
        logger.debug("[route:%s] stopped processing route" % self.name)

class RouteManager(MultiService):
    def __init__(self):
        MultiService.__init__(self)
        self.setName("routes")
        self._routes = []

    def configure(self, settings):
        # configure each route, composed of an input, filters, and an output
        for section in settings.sectionsLike('route:'):
            name = section.name.split(':',1)[1]
            try:
                route = Route(name)
                route.configure(section)
                self._routes.append(route)
            except Exception, e:
                logger.warning("failed to load route %s: %s" % (name, e))

    def stopService(self):
        for route in self._routes:
            route.close()
        self._routes = []
        return MultiService.stopService(self)


routes = RouteManager()
