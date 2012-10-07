# Copyright 2010,2011,2012 Michael Frank <msfrank@syntaxjockey.com>
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

import time
from zope.interface import implements
from twisted.internet.defer import succeed
from twisted.python.failure import Failure
from terane import IManager, Manager
from terane.registry import getRegistry
from terane.bier.evid import EVID
from terane.bier.ql import parseIterQuery, parseTailQuery
from terane.bier.searching import searchIndices, Period, SearcherError
from terane.loggers import getLogger

logger = getLogger('terane.bier')

class QueryExecutionError(Exception):
    """
    There was an error while executing the plan.
    """
    pass

class QueryResult(object):
    def __init__(self, meta={}, data=[]):
        self.meta = meta
        self.data = data
    def __str__(self):
        return "<QueryResult meta=%s, data=%s>" % (self.meta, self.data)

class QueryManager(Manager):

    implements(IManager)

    def __init__(self):
        Manager.__init__(self)
        self._searchables = {}
        self.maxResultSize = 10
        self.maxIterations = 5

    def configure(self, settings):
        pass

    def startService(self):
        Manager.startService(self)
        routes = getRegistry().getComponent(IManager, 'routes')
        for output in routes.listSearchables():
            index = output.getIndex()
            self._searchables[output.name] = index
        if len(self._searchables) < 1:
            logger.info("no searchable indices found")
        else:
            logger.info("found searchable indices: %s" % ', '.join(self._searchables.keys()))

    def stopService(self):
        self._searchables = None
        return Manager.stopService(self)

    def iter(self, query, lastId=None, indices=None, limit=100, reverse=False, fields=None):
        """
        Iterate through the database for events matching the specified query.

        :param query: The query string.
        :type query: unicode
        :param lastId: The ID of the last event from the previous iteration.
        :type lastId: str
        :param indices: A list of indices to search, or None to search all indices.
        :type indices: list, or None
        :param limit: The maximum number of events to return.
        :type limit: int
        :param reverse: Whether to return last results first.
        :type reverse: bool
        :param fields: A list of fields to return in the results, or None to return all fields.
        :type fields: list
        :returns: A Deferred object which receives the results.
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        # look up the named indices
        if indices == None:
            indices = self._searchables.values()
        else:
            try:
                indices = tuple(self._searchables[name] for name in indices)
            except KeyError, e:
                raise QueryExecutionError("unknown index '%s'" % e)
        # if lastId is specified, make sure its a valid value
        if lastId != None and not isinstance(lastId, EVID):
            raise QueryExecutionError("lastId is not valid")
        # check that limit is > 0
        if limit < 1:
            raise QueryExecutionError("limit must be greater than 0")
        query,period = parseIterQuery(query)
        logger.trace("iter query: %s" % query)
        logger.trace("iter period: %s" % period)
        # query each index and return the results
        try:
            task = searchIndices(indices, query, period, lastId, reverse, fields, limit)
        except SearcherError, e:
            raise QueryExecutionError(str(e))
        def _returnIterResult(result):
            if isinstance(result, Failure) and result.check(SearcherError):
                raise QueryExecutionError(str(e))
            return QueryResult({'runtime': result.runtime, 'fields': result.fields}, result.events)
        return task.whenDone().addBoth(_returnIterResult)

    def tail(self, query, lastId=None, indices=None, limit=100, fields=None):
        """
        Return events newer than the specified 'lastId' event ID matching the
        specified query.

        :param query: The query string.
        :type query: unicode
        :param lastId: The ID of the last event from the previous iteration.
        :type lastId: str
        :param indices: A list of indices to search, or None to search all indices.
        :type indices: list, or None
        :param limit: The maximum number of events to return.
        :type limit: int
        :param fields: A list of fields to return in the results, or None to return all fields.
        :type fields: list
        :returns: A Deferred object which receives the results.
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        # look up the named indices
        if indices == None:
            indices = self._searchables.values()
        else:
            try:
                indices = tuple(self._searchables[name] for name in indices)
            except KeyError, e:
                raise QueryExecutionError("unknown index '%s'" % e)
        # if lastId is 0, return the id of the latest document
        if lastId == None:
            return QueryResult({'runtime': 0.0, 'lastId': str(EVID.fromDatetime())}, [])
        try:
            lastId = EVID.fromString(lastId)
        except:
            raise QueryExecutionError("invalid lastId '%s'" % str(lastId))
        # check that limit is > 0
        if limit < 1:
            raise QueryExecutionError("limit must be greater than 0")
        query = parseTailQuery(query)
        logger.trace("tail query: %s" % query)
        period = Period(lastId, EVID.fromString(EVID.MAX_ID), True, False)
        logger.trace("tail period: %s" % period)
        # query each index, and return the results
        task = searchIndices(indices, query, period, None, False, fields, limit)
        def _returnTailResult(result, lastId=None):
            if isinstance(result, Failure) and result.check(SearcherError):
                raise QueryExecutionError(str(e))
            events = list(result.events)
            if len(events) > 0:
                lastId = events[-1][0]
            metadata = {'runtime': result.runtime, 'lastId': str(lastId), 'fields': result.fields}
            return QueryResult(metadata, events)
        return task.whenDone().addBoth(_returnTailResult, lastId)

    def showIndex(self, name):
        """
        Return metadata about the specified index.  Currently the only information
        returned is the number of events in the index, the last-modified time (as a
        unix timestamp), and the event ID of the latest event.

        :param name: The name of the index.
        :type name: unicode
        :returns: A Deferred object which receives the results.
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        try:
            index = self._searchables[name]
        except KeyError, e:
            raise QueryExecutionError("unknown index '%s'" % e)
        return succeed(QueryResult(index.getStats(), index.schema().listFields()))

    def listIndices(self):
        """
        Return a list of names of the indices present.

        :returns: A Deferred object which receives the results.
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        return succeed(QueryResult({}, self._searchables.keys()))
