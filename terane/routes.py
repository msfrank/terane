#Copyright 2011,2011 Michael Frank <msfrank@syntaxjockey.com>
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
from dateutil.tz import tzutc
from twisted.application.service import MultiService
from twisted.internet.task import cooperate
from terane.plugins import plugins
from terane.filters import FilterError, StopFiltering
from terane.signals import SignalCancelled
from terane.settings import ConfigureError
from terane.loggers import getLogger

logger = getLogger('terane.routes')

class RoutingError(Exception):
    pass

class EventProcessor(object):
    def __init__(self, event, filters):
        self.event = event
        self._filters = filters

    def next(self):
        if len(self._filters) == 0:
            raise StopIteration()
        filter = self._filters.pop(0)
        self.event = filter.getContract().validateEvent(filter.filter(self.event))

class Route(object):
    """
    A Route describes the flow of an event stream.  It consists of a single
    input, zero or more filters, and a single output.
    """

    def __init__(self, name):
        self.name = name

    def configure(self, section):
        chain = []
        # load the route input
        name = section.getString('input')
        try:
            self._input = plugins.instance('input', name)
        except:
            raise ConfigureError("no such input %s" % name)
        chain.append(self._input)
        # load the route filter chain
        self._filters = []
        filters = section.getString('filter', '').strip()
        filters = [f.strip() for f in filters.split('|') if not f == '']
        if len(filters) > 0:
            # verify each referenced filter has been loaded
            for name in filters:
                try:
                    filter = plugins.instance('filter', name)
                except:
                    raise ConfigureError("no such filter %s" % name)
                # add filter to the filter chain
                self._filters.append(filter)
                chain.append(filter)
        # load the route output
        name = section.getString('output')
        try:
            self._output = plugins.instance('output', name)
        except:
            raise ConfigureError("no such output %s" % name)
        chain.append(self._output)
        # verify that the filtering chain will work
        aggregate = chain[0].getContract()
        for i in range(1, len(chain)):
            try:
                contract = chain[i].getContract()
                aggregate = contract.validateContract(aggregate)
            except Exception, e:
                raise ConfigureError("element #%i: %s" % (i, e))
        logger.debug("[route:%s] route configuration: %s" %
            (self.name, ' -> '.join([e.name for e in chain])))
        # schedule the on_received_event signal
        self._scheduleReceivedEvent()

    def _scheduleReceivedEvent(self):
        self.d = self._input.getDispatcher().connect()
        self.d.addCallbacks(self._receivedEvent, lambda failure: failure)
        self.d.addErrback(self._errorReceivingEvent)

    def _receivedEvent(self, event):
        # run the fields through the filter chain, then reschedule the signal
        self._input.getContract().validateEvent(event)
        task = cooperate(EventProcessor(event, self._filters))
        task.whenDone().addCallbacks(self._processedEvent, self._errorProcessingEvent)
        self._scheduleReceivedEvent()

    def _errorReceivingEvent(self, failure):
        if not failure.check(SignalCancelled):
            logger.debug("[route:%s] error receiving event: %s" % (self.name,str(failure)))
            self._scheduleReceivedEvent()
            return failure

    def _processedEvent(self, processor):
        logger.debug("[route:%s] processed event" % self.name)
        self._output.receiveEvent(processor.event)

    def _errorProcessingEvent(self, failure):
        if not failure.check(StopFiltering):
            logger.debug("[route:%s] error processing event: %s" % (self.name,str(failure)))
            return failure
        logger.debug("[route:%s] dropped event: %s" % (self.name,e))

    def close(self):
        if self.d != None:
            self._input.getDispatcher().disconnect(self.d)
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
            except ConfigureError, e:
                logger.warning("failed to load route %s: %s" % (name, e))

    def stopService(self):
        for route in self._routes:
            route.close()
        self._routes = []
        return MultiService.stopService(self)


routes = RouteManager()
