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

from zope.interface import Interface, implements
from twisted.application.service import Service
from twisted.internet.task import cooperate
from terane.manager import IManager, Manager
from terane.plugins import IPluginStore
from terane.bier import IEventFactory, IFieldStore
from terane.inputs import IInput
from terane.outputs import IOutput, ISearchable
from terane.filters import IFilter, StopFiltering
from terane.signals import SignalCancelled
from terane.settings import ConfigureError
from terane.loggers import getLogger

logger = getLogger('terane.routes')

class EventProcessor(object):
    def __init__(self, event, filters, fieldstore):
        self.event = event
        self._filters = filters
        self._fieldstore = fieldstore

    def next(self):
        if len(self._filters) == 0:
            raise StopIteration()
        filter = self._filters.pop(0)
        contract = filter.getContract()
        contract.validateEventBefore(self.event, self._fieldstore)
        self.event = filter.filter(self.event)
        contract.validateEventAfter(self.event, self._fieldstore)

class Route(Service):
    """
    A Route describes the flow of an event stream.  It consists of a single
    input, zero or more filters, and a single output.
    """

    def __init__(self, name):
        self.setName(name)

    def configure(self, section):
        # load the route input
        name = section.getString('input')
        if not name in self.parent._inputs:
            raise ConfigureError("route %s requires unknown input %s" % (self.name,name))
        self._input = self.parent._inputs[name]
        # load the route filter chain
        self._filters = []
        filters = section.getString('filter', '').strip()
        filters = [f.strip() for f in filters.split('|') if not f == '']
        if len(filters) > 0:
            # verify each referenced filter has been loaded
            for name in filters:
                if not name in self.parent._filters:
                    raise ConfigureError("route %s requires unknown filter %s" % (self.name,name))
                self._filters.append(self.parent._filters[name])
        # load the route output
        name = section.getString('output')
        if not name in self.parent._outputs:
            raise ConfigureError("route %s requires unknown output %s" % (self.name,name))
        self._output = self.parent._outputs[name]
        # verify that the route will work
        chain = [self._input] + [f for f in self._filters] + [self._output]
        self._final = chain[0].getContract()
        for i in range(1, len(chain)):
            try:
                contract = chain[i].getContract()
                self._final = contract.validateContract(self._final)
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
        self._input.getContract().validateEventAfter(event, self.parent._fieldstore)
        task = cooperate(EventProcessor(event, self._filters, self.parent._fieldstore))
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
        self._output.getContract().validateEventBefore(processor.event, self.parent._fieldstore)
        event = self._final.finalizeEvent(processor.event)
        self._output.receiveEvent(event)

    def _errorProcessingEvent(self, failure):
        if not failure.check(StopFiltering):
            logger.debug("[route:%s] error processing event: %s" % (self.name,str(failure)))
            return failure
        logger.debug("[route:%s] dropped event: %s" % (self.name,e))

class IIndexStore(Interface):
    def getSearchableIndex(name):
        """
        Return the searchable index specified by name.

        :param name: The name of the index.
        :type name: str
        :returns: The index.
        :rtype: An object providing :class:`terane.outputs.ISearchable`
        :raises KeyError: The specified index does not exist.
        """
    def iterSearchableIndices():
        """
        Iterate the searchable indices.

        :returns: An iterator yielding objects providing :class:`terane.outputs.ISearchable`.
        :rtype: iter
        """
    def iterSearchableNames():
        """
        Iterate the searchable index names.

        :returns: An iterator yielding strings containing searchable index names.
        :rtype: iter
        """

class RouteManager(Manager):
    """
    """

    implements(IManager, IIndexStore)

    def __init__(self, pluginstore, eventfactory, fieldstore):
        if not IPluginStore.providedBy(pluginstore):
            raise TypeError("pluginstore class does not implement IPluginStore")
        if not IEventFactory.providedBy(eventfactory):
            raise TypeError("eventfactory class does not implement IEventFactory")
        if not IFieldStore.providedBy(fieldstore):
            raise TypeError("fieldstore class does not implement IFieldStore")
        Manager.__init__(self)
        self.setName("routes")
        self._pluginstore = pluginstore
        self._eventfactory = eventfactory
        self._fieldstore = fieldstore
        self._routes = {}
        self._inputs = {}
        self._filters = {}
        self._outputs = {}
        self._searchables = {}

    def configure(self, settings):
        # configure each input
        for section in settings.sectionsLike("input:"):
            name = section.name.split(':',1)[1]
            if name in self._inputs:
                raise ConfigureError("input %s was already defined" % name)
            type = section.getString('type', None)
            if type == None:
                raise ConfigureError("input %s is missing required parameter 'type'" % name)
            try:
                factory = self._pluginstore.getComponent(IInput, type)
            except KeyError:
                raise ConfigureError("no input found for type '%s'" % type)
            input = factory(name, self._eventfactory)
            input.configure(section)
            self._inputs[name] = input
        # configure each filter
        for section in settings.sectionsLike("filter:"):
            name = section.name.split(':',1)[1]
            if name in self._filters:
                raise ConfigureError("filter %s was already defined" % name)
            type = section.getString('type', None)
            if type == None:
                raise ConfigureError("filter %s is missing required parameter 'type'" % name)
            try:
                factory = self._pluginstore.getComponent(IFilter, type)
            except KeyError:
                raise ConfigureError("no filter found for type '%s'" % type)
            filter = factory(name)
            filter.configure(section)
            self._filters[name] = filter
        # configure each output
        for section in settings.sectionsLike("output:"):
            name = section.name.split(':',1)[1]
            if name in self._outputs:
                raise ConfigureError("output %s was already defined" % name)
            type = section.getString('type', None)
            if type == None:
                raise ConfigureError("output %s is missing required parameter 'type'" % name)
            try:
                factory = self._pluginstore.getComponent(IOutput, type)
            except KeyError:
                raise ConfigureError("no input found for type '%s'" % type)
            output = factory(name, self._fieldstore)
            output.configure(section)
            self._outputs[name] = output
            if ISearchable.providedBy(output):
                self._searchables[name] = output
        if len(self._searchables) < 1:
            logger.info("no searchable indices found")
        else:
            logger.info("found searchable indices: %s" % ', '.join(self._searchables.keys()))
        # configure each route
        for section in settings.sectionsLike('route:'):
            name = section.name.split(':',1)[1]
            if name in self._routes:
                raise ConfigureError("route %s was already defined" % name)
            route = Route(name)
            route.setServiceParent(self)
            try:
                route.configure(section)
            except ConfigureError, e:
                route.disownServiceParent()
                logger.warning("failed to load route %s: %s" % (name, e))

    def startService(self):
        Manager.startService(self)
        for output in self._outputs.values():
            output.startService()
        for input in self._inputs.values():
            input.startService()

    def stopService(self):
        for input in self._inputs.values():
            input.stopService()
        for output in self._outputs.values():
            output.stopService()
        return Manager.stopService(self)

    def getSearchableIndex(self, name):
        """
        Return the searchable index specified by name.

        :param name: The name of the index.
        :type name: str
        :returns: The index.
        :rtype: An object providing :class:`terane.outputs.ISearchable`
        :raises KeyError: The specified index does not exist.
        """
        return self._searchables[name].getIndex()
                                        
    def iterSearchableIndices(self):
        """
        Iterate the searchable indices.

        :returns: An iterator yielding objects providing :class:`terane.outputs.ISearchable`.
        :rtype: iter
        """
        return self._searchables.itervalues()

    def iterSearchableNames(self):
        """
        Iterate the searchable index names.

        :returns: An iterator yielding strings containing searchable index names.
        :rtype: iter
        """
        return self._searchables.iterkeys() 
