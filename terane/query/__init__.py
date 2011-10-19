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
from pyparsing import ParseBaseException
from terane.plugins import plugins
from terane.outputs import ISearchableOutput
from terane.query.query import searchQuery
from terane.query.results import Results
from terane.loggers import getLogger

logger = getLogger('terane.query')

class QuerySyntaxError(BaseException):
    """
    There was an error parsing the query synatx.
    """
    def __init__(self, exc, qstring):
        self._exc = exc
        tokens = qstring[exc.col-1:].splitlines()[0]
        self._message = "Syntax error starting at '%s': %s (line %i, col %i)" % (
            tokens, exc.msg, exc.lineno, exc.col)
    def __str__(self):
        return self._message

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

    def parseSearchQuery(self, string):
        """
        Parse the query specified by qstring.  Returns a Query object.
        """
        try:
            return searchQuery.parseString(string, parseAll=True).asList()[0]
        except ParseBaseException, e:
            raise QuerySyntaxError(e, string)

    def parseRestriction(self, string):
        """
        Parse the restriction specified by rstring.  Returns a Restriction object.
        """
        return None

    def execute(self, query, indices=None, limit=100, restrictions=None, sorting=None, reverse=False, fields=None):
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
        # FIXME: check each list item to make sure its in at least 1 schema
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

    def explain(self, query, indices=None, limit=100, restrictions=None, sorting=None, reverse=False, fields=None):
        pass

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
