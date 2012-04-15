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
from twisted.application.service import Service
from terane.plugins import plugins
from terane.outputs import ISearchableOutput
from terane.bier.evid import EVID
from terane.bier.ql import parseIterQuery, parseTailQuery
from terane.bier.searching import searchIndices, Period
from terane.loggers import getLogger

logger = getLogger('terane.bier')

class QueryExecutionError(Exception):
    """
    There was an error while executing the plan.
    """
    pass

class QueryManager(Service):
    def __init__(self):
        self._searchables = {}
        self.maxResultSize = 10
        self.maxIterations = 5

    def configure(self, settings):
        pass

    def startService(self):
        Service.startService(self)
        for output in plugins.instancesImplementing('output', ISearchableOutput):
            index = output.index()
            self._searchables[output.name] = index
        if len(self._searchables) < 1:
            logger.info("no searchable indices found")
        else:
            logger.info("found searchable indices: %s" % ', '.join(self._searchables.keys()))

    def stopService(self):
        self._searchables = None
        return Service.stopService(self)

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
        :returns:
        :rtype:
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
        runtime = time.time()
        results,fields = searchIndices(indices, query, period, lastId, reverse, fields, limit)
        runtime = time.time() - runtime
        return list(results), {'runtime': runtime, 'fields': fields}

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
        :returns: A Results object containing the search results.
        :rtype: :class:`terane.query.results.Results`
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
            return [], {'runtime': 0.0, 'last-id': str(EVID.fromDatetime())}
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
        runtime = time.time()
        results,fields = searchIndices(indices, query, period, None, True, fields, limit)
        runtime = time.time() - runtime
        try:
            lastId = results[-1][0]
        except IndexError:
            pass
        return list(results), {'runtime': runtime, 'last-id': str(lastId), 'fields': fields}

    def showIndex(self, name):
        """
        Return metadata about the specified index.  Currently the only information
        returned is the number of events in the index, the last-modified time (as a
        unix timestamp), and the event ID of the latest event.

        :param name: The name of the index.
        :type name: unicode
        :returns: A Results object containing the index metadata.
        :rtype: :class:`terane.query.results.Results`
        """
        try:
            index = self._searchables[name]
        except KeyError, e:
            raise QueryExecutionError("unknown index '%s'" % e)
        stats = index.getStats()
        return index.schema().listFields(), stats

    def listIndices(self):
        """
        Return a list of names of the indices present.

        :returns: A Results object containing a list of index names.
        :rtype: :class:`terane.query.results.Results`
        """
        return self._searchables.keys(), {}


queries = QueryManager()
"""
`queries` is a singleton instance of a :class:`QueryManager`.  All interaction
with the query infrastructure must occur through this instance; do *not* instantiate
new :class:`QueryManager` instances!
"""
