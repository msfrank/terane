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

from zope.interface import implements
from twisted.application.service import Service
from twisted.internet.task import cooperate
from terane import IManager, Manager
from terane.registry import getRegistry
from terane.inputs import IInput
from terane.outputs import IOutput, ISearchable
from terane.filters import IFilter, StopFiltering
from terane.signals import SignalCancelled
from terane.settings import ConfigureError
from terane.loggers import getLogger

logger = getLogger('terane.routes')

class EventProcessor(object):
    def __init__(self, event, filters):
        self.event = event
        self._filters = filters

    def next(self):
        if len(self._filters) == 0:
            raise StopIteration()
        filter = self._filters.pop(0)
        contract = filter.getContract()
        contract.validateEventBefore(self.event)
        self.event = filter.filter(self.event)
        contract.validateEventAfter(self.event)

class Route(Service):
    """
    A Route describes the flow of an event stream.  It consists of a single
    input, zero or more filters, and a single output.
    """

    def configure(self, section):
        manager = self.parent
        # load the route input
        name = section.getString('input')
        if not name in manager.inputs:
            raise ConfigureError("route %s requires unknown input %s" % (self.name,name))
        self._input = manager.inputs[name]
        # load the route filter chain
        self._filters = []
        filters = section.getString('filter', '').strip()
        filters = [f.strip() for f in filters.split('|') if not f == '']
        if len(filters) > 0:
            # verify each referenced filter has been loaded
            for name in filters:
                if not name in manager.filters:
                    raise ConfigureError("route %s requires unknown filter %s" % (self.name,name))
                self._filters.append(manager.filters[name])
        # load the route output
        name = section.getString('output')
        if not name in manager.outputs:
            raise ConfigureError("route %s requires unknown output %s" % (self.name,name))
        self._output = manager.outputs[name]
        # verify that the route will work
        chain = [self._input] + [f for f in self._filters] + [self._output]
        aggregate = chain[0].getContract()
        for i in range(1, len(chain)):
            try:
                contract = chain[i].getContract()
                aggregate = contract.validateContract(aggregate)
            except Exception, e:
                raise ConfigureError("element #%i: %s" % (i, e))
        logger.debug("[route:%s] route configuration: %s" %
            (self.name, ' -> '.join([e.name for e in chain])))

    def startService(self):
        # schedule the on_received_event signal
        self._scheduleReceivedEvent()

    def stopService(self):
        if self.d != None:
            self._input.getDispatcher().disconnectSignal(self.d)
            self.d = None
        logger.debug("[route:%s] stopped processing route" % self.name)

    def _scheduleReceivedEvent(self):
        self.d = self._input.getDispatcher().connectSignal()
        self.d.addCallbacks(self._receivedEvent, lambda failure: failure)
        self.d.addErrback(self._errorReceivingEvent)

    def _receivedEvent(self, event):
        # run the fields through the filter chain, then reschedule the signal
        self._input.getContract().validateEventAfter(event)
        task = cooperate(EventProcessor(event, self._filters))
        d = task.whenDone()
        d.addCallbacks(self._processedEvent, lambda failure: failure)
        d.addErrback(self._errorProcessingEvent)
        self._scheduleReceivedEvent()

    def _errorReceivingEvent(self, failure):
        if not failure.check(SignalCancelled):
            logger.debug("[route:%s] error receiving event: %s" % (self.name,str(failure)))
            self._scheduleReceivedEvent()
            return failure

    def _processedEvent(self, processor):
        logger.debug("[route:%s] processed event" % self.name)
        self._output.getContract().validateEventBefore(processor.event)
        self._output.receiveEvent(processor.event)

    def _errorProcessingEvent(self, failure):
        if not failure.check(StopFiltering):
            logger.debug("[route:%s] error processing event: %s" % (self.name,str(failure)))
            return failure
        logger.debug("[route:%s] dropped event: %s" % (self.name,e))

class RouteManager(Manager):

    implements(IManager)

    def __init__(self):
        Manager.__init__(self)
        self.setName("routes")
        self.routes = {}
        self.inputs = {}
        self.filters = {}
        self.outputs = {}
        self.searchables = []

    def configure(self, settings):
        registry = getRegistry()
        # configure each input, filter, output
        for ptype,pname,components in [
          (IInput, 'input', self.inputs),
          (IFilter, 'filter', self.filters),
          (IOutput, 'output', self.outputs)]:
            for section in settings.sectionsLike("%s:" % pname):
                cname = section.name.split(':',1)[1]
                if cname in components:
                    raise ConfigureError("%s %s was already defined" % (pname, cname))
                ctype = section.getString('type', None)
                if ctype == None:
                    raise ConfigureError("%s %s is missing required parameter 'type'" % (pname,cname))
                try:
                    factory = registry.getComponent(ptype, ctype)
                except KeyError:
                    raise ConfigureError("no %s found for type '%s'" % (pname,ctype))
                component = factory()
                component.setName(cname)
                component.configure(section)
                components[cname] = component
        # find the searchable outputs
        for output in self.outputs.itervalues():
            if ISearchable.providedBy(output):
                self.searchables.append(output)
        # configure each route
        for section in settings.sectionsLike('route:'):
            name = section.name.split(':',1)[1]
            if name in self.routes:
                raise ConfigureError("route %s was already defined" % name)
            route = Route()
            route.setName(name)
            route.setServiceParent(self)
            try:
                route.configure(section)
            except ConfigureError, e:
                route.disownServiceParent()
                logger.warning("failed to load route %s: %s" % (name, e))

    def startService(self):
        Manager.startService(self)
        for output in self.outputs.values():
            output.startService()
        for input in self.inputs.values():
            input.startService()

    def stopService(self):
        for input in self.inputs.values():
            input.stopService()
        for output in self.outputs.values():
            output.stopService()
        return Manager.stopService(self)

    def listSearchables(self):
        return self.searchables
