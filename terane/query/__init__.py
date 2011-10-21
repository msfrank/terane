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

from twisted.application.service import Service
from whoosh.query import NumericRange, And
from terane.plugins import plugins
from terane.outputs import ISearchableOutput
from terane.query.dql import parseSearchQuery, parseTailQuery
from terane.query.results import Results
from terane.loggers import getLogger

logger = getLogger('terane.query')

class QueryExecutionError(Exception):
    """
    There was an error while executing the plan.
    """
    pass

class QueryManager(Service):
    def __init__(self):
        self._searchables = {}

    def configure(self, settings):
        pass

    def startService(self):
        Service.startService(self)
        for index in plugins.instancesImplementing('output', ISearchableOutput):
            self._searchables[index.name] = index
        logger.debug("found searchable indices: %s" % ', '.join(self._searchables.keys()))

    def stopService(self):
        self._searchables = None
        return Service.stopService(self)

    def search(self, query, indices=None, limit=100, restrictions=None, sorting=None, reverse=False, fields=None):
        query = parseSearchQuery(query)
        # look up the named indices
        if indices == None:
            indices = self._searchables.values()
        else:
            try:
                indices = tuple(self._searchables[name] for name in indices)
            except KeyError, e:
                raise QueryExecutionError("unknown index '%s'" % e)
        # check that limit is > 0
        if limit < 1:
            raise QueryExecutionError("limit must be greater than 0")
        # FIXME: check that restrictions is a Restrictions object
        # FIXME: check each fields item to make sure its in at least 1 schema
        # query each index, and aggregate the results
        try:
            results = Results(sorting, fields, reverse)
            rlist = []
            runtime = 0.0
            for index in indices:
                result = index.search(query, limit, sorting, reverse)
                rlist.append(result)
                runtime += result.runtime
            results.extend(*rlist, runtime=runtime)
            # FIXME: check whether results satisfies all restrictions
            return results
        except Exception, e:
            raise QueryExecutionError(str(e))

    def tail(self, query, last, indices=None, limit=100, fields=None):
        # look up the named indices
        if indices == None:
            indices = self._searchables.values()
        else:
            try:
                indices = tuple(self._searchables[name] for name in indices)
            except KeyError, e:
                raise QueryExecutionError("unknown index '%s'" % e)
        # check that limit is > 0
        if limit < 1:
            raise QueryExecutionError("limit must be greater than 0")
        # FIXME: check each fields item to make sure its in at least 1 schema
        try:
            results = Results(("ts"), fields, False)
            # determine the id of the last document
            lastId = 0
            for index in indices:
                l = index.lastId()
                if l > lastId: lastId = l
            # if last is 0, then return the id of the latest document
            if last == 0:
                results.extend(last=lastId, runtime=0.0)
                return results
            # if the latest document id is smaller or equal to supplied last id value,
            # then return the id of the latest document
            if lastId <= last:
                results.extend(last=lastId, runtime=0.0)
                return results
            query = parseTailQuery(query)
            # add the additional restriction that the id must be newer than 'last'.
            query = And([NumericRange('id', last, 2**64, True), query]).normalize()
            # query each index, and aggregate the results
            rlist = []
            runtime = 0.0
            lastId = 0
            # query each index, and aggregate the results
            for index in indices:
                result = index.search(query, limit, ("ts"), False)
                rlist.append(result)
                runtime += result.runtime
                try:
                    l = result.docnum(-1)
                    if l > lastId: lastId = l
                except IndexError:
                    # if there are no results, result.docnum() raises IndexError
                    pass
            results.extend(*rlist, last=lastId, runtime=runtime)
            return results
        except Exception, e:
            raise QueryExecutionError(str(e))

    def showIndex(self, name):
        try:
            index = self._searchables[name]
        except KeyError, e:
            raise QueryExecutionError("unknown index '%s'" % e)
        results = Results(None, None, False)
        meta = {}
        meta['name'] = name
        meta['size'] = index.size()
        meta['last-modified'] = index.lastModified()
        meta['last-id'] = index.lastId()
        results.extend([{'field':{'value':name}} for name in index.schema.names()], **meta)
        return results

    def listIndices(self):
        indices = tuple(self._searchables.keys())
        results = Results(None, None, False)
        results.extend([{'index':{'value':v}} for v in indices])
        return results


queries = QueryManager()
