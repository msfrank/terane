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

import socket, datetime, traceback
from twisted.application.service import MultiService
from terane.plugins import plugins
from terane.filters import FilterError, StopFiltering
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
        if not name in routes._inputs:
            raise ConfigureError("no such input %s" % name)
        self._input = routes._inputs[name]
        # load the route output
        name = section.getString('output')
        if not name in routes._outputs:
            raise ConfigureError("no such output %s" % name)
        self._output = routes._outputs[name]
        # load the route filter chain
        self._filters = []
        filters = section.getString('filter', '').strip()
        filters = [f.strip() for f in filters.split('|') if not f == '']
        if len(filters) > 0:
            # verify each referenced filter has been loaded
            for name in filters:
                if not name in routes._filters:
                    raise ConfigureError("no such filter %s" % name)
                # add filter to the filter chain
                self._filters.append(routes._filters[name])
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
                logger.warn("[route:%s] input %s does not guarantee fields required by filter %s, events may be lost" %
                    (self.name, self._input.name, self._filters[0].name))
            # warn if the output may not receive all required fields
            if not self._output.infields().issubset(requiredfields):
                logger.warn("[route:%s] filter chain does not guarantee fields required by output %s, events may be lost" %
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
        logger.debug("[route:%s] error processing event: %s" % (self.name,str(failure)))
        self._scheduleReceivedEvent()
        return failure

class RouteManager(MultiService):
    def __init__(self):
        MultiService.__init__(self)
        self.setName("routes")
        self._inputs = {}
        self._filters = {}
        self._outputs = {}
        self._routes = []

    def configure(self, settings):
        # configure each output
        for section in settings.sectionsLike('output:'):
            name = section.name.split(':',1)[1]
            try:
                sink_type = section.getString('type', None)
                if sink_type == None:
                    raise Exception("missing required option 'type'")
                sink = plugins.instance('output', sink_type)
                sink.setName(name)
                sink.configure(section)
                self.addService(sink)
                self._outputs[name] = sink
            except Exception, e:
                tb = "\nUnhandled Exception:\n%s\n---\n%s" % (e,traceback.format_exc())
                logger.warning("failed to load output '%s':%s" % (name, tb))
        # configure each input
        for section in settings.sectionsLike('input:'):
            name = section.name.split(':',1)[1]
            try:
                source_type = section.getString('type', None)
                if source_type == None:
                    raise Exception("missing required option 'type'")
                source = plugins.instance('input', source_type)
                source.setName(name)
                source.configure(section)
                self.addService(source)
                self._inputs[name] = source
            except Exception, e:
                tb = "\nUnhandled Exception:\n%s\n---\n%s" % (e,traceback.format_exc())
                logger.warning("failed to load input '%s':%s" % (name, tb))
        # configure each filter
        for section in settings.sectionsLike('filter:'):
            name = section.name.split(':',1)[1]
            try:
                filter_type = section.getString('type', None)
                if filter_type == None:
                    raise Exception("missing required option 'type'")
                filter = plugins.instance('filter', filter_type)
                filter.setName(name)
                filter.configure(section)
                self._filters[name] = filter
            except Exception, e:
                tb = "\nUnhandled Exception:\n%s\n---\n%s" % (e,traceback.format_exc())
                logger.warning("failed to load filter '%s':%s" % (name, tb))
        # configure each route, composed of an input, filters, and an output
        for section in settings.sectionsLike('route:'):
            name = section.name.split(':',1)[1]
            try:
                route = Route(name)
                route.configure(section)
                self._routes.append(route)
            except Exception, e:
                logger.warning("failed to load route %s: %s" % (name, e))


routes = RouteManager()
