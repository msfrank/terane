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

from whoosh.query import Query, NumericRange
from terane.dql.joint import And
from terane.db import db
from terane.loggers import getLogger

logger = getLogger('terane.db.plan')

class PlanConfigurationError(Exception):
    """
    There was an error while building the execution plan.
    """
    pass

class PlanExecutionError(Exception):
    """
    There was an error while executing the plan.
    """
    pass

class ExecutionPlan(object):
    """
    Defines the interface for execution plans.
    """

    fields = None

    reverse = False

    sorting = None

    def execute(self):
        pass

class SearchPlan(ExecutionPlan):

    def __init__(self, query, indices=None, limit=100, restrictions=None, sorting=None, reverse=False, fields=None):
        # check that query is a Query object
        if not isinstance(query, Query):
            raise PlanConfigurationError("query must be of type whoosh.query.Query")
        self.query = query
        # look up the named indices
        if indices == None:
            self.indices = db._indices.values()
        else:
            self.indices = []
            try:
                self.indices = tuple(db._indices[name] for name in indices)
            except KeyError, e:
                raise PlanConfigurationError("unknown index '%s'" % e)
            except TypeError:
                raise PlanConfigurationError("indices must be a sequence of strings, or None")
        # check that limit is > 0
        if limit < 1:
            raise PlanConfigurationError("limit must be greater than 0")
        self.limit = limit
        # FIXME: check that restrictions is a Restrictions object
        self.restrictions = None
        # check that the sorting spec is valid
        if sorting == None:
            self.sorting = None
        else:
            try:
                # FIXME: should we check each list item to make sure its in at least 1 schema?
                self.sorting = tuple(field for field in sorting)
            except TypeError:
                raise PlanConfigurationError("sorting must be a sequence of strings, or None")
        # set the reverse flag
        self.reverse = bool(reverse)
        # check that the fields spec is valid
        if fields == None:
            self.fields = None
        else:
            try:
                self.fields = tuple(field for field in fields)
            except TypeError:
                raise PlanConfigurationError("fields must be a sequence of strings, or None")

    def execute(self):
        """
        Search the database using the plan.
        
        :returns: The search results.
        :rtype: :class:`Results`
        """
        # query each index, and aggregate the results
        try:
            rlist = []
            runtime = 0.0
            for i in self.indices:
                result = i.search(self.query, self.limit, self.sorting, self.reverse)
                rlist.append(result)
                runtime += result.runtime
            results = Results(self, *rlist, runtime=runtime)
            # check whether results satisfies all restrictions
            if not self.restrictions or self.restrictions.isSatisfied(results):
                return results
            # if restrictions aren't satisfied, return empty set
            return Results(self, runtime=runtime)
        except Exception, e:
            raise PlanExecutionError(str(e))

class ExplainQueryPlan(SearchPlan):

    def execute(self):
        """
        Describe how the search plan would work.

        :returns: The explanation, as a :class:`Results` object with a single row.
        :rtype: :class:`Results`
        """
        try:
            estimate = 0
            estimate_min = 0
            fields = []
            for i in self.indices:
                fields += [f for f in i.schema.names() if not f in fields]
                reader = i.reader()
                estimate += self.query.estimate_size(reader)
                estimate_min += self.query.estimate_min_size(reader)
            return Results(self, fields, estimate=estimate, estimate_min=estimate_min)
        except Exception, e:
            raise PlanExecutionError(str(e))


class TailPlan(ExecutionPlan):

    _MAXID = 2**64

    def __init__(self, query, last, indices=None, limit=100, fields=None):
        # check that query is a Query object
        if not isinstance(query, Query):
            raise PlanConfigurationError("query must be of type whoosh.query.Query")
        self.query = query
        try:
            self.last = long(last)
        except ValueError:
            raise PlanConfigurationError("last must be a number")
        if self.last < 0:
            raise PlanConfigurationError("last must 0 or greater")
        # look up the named indices
        if indices == None:
            self.indices = db._indices.values()
        else:
            self.indices = []
            try:
                self.indices = tuple(db._indices[name] for name in indices)
            except KeyError, e:
                raise PlanConfigurationError("unknown index '%s'" % e)
            except TypeError:
                raise PlanConfigurationError("indices must be a sequence of strings, or None")
        # check that limit is > 0
        if limit < 1:
            raise PlanConfigurationError("limit must be greater than 0")
        self.limit = limit
        # check that the fields spec is valid
        if fields == None:
            self.fields = None
        else:
            try:
                self.fields = tuple(field for field in fields)
            except TypeError:
                raise PlanConfigurationError("fields must be a sequence of strings, or None")
        # these need to be set even tho they are hardcoded, because the
        # Results class needs to use them
        self.sorting = ('id',)
        self.reverse = False

    def execute(self):
        try:
            # determine the id of the last document
            last = 0
            for i in self.indices:
                l = i.last_id()
                if l > last: last = l
            # if last is 0, then return the id of the latest document
            if self.last == 0:
                return Results(self, last=last, runtime=0.0)
            # if the latest document id is smaller or equal to supplied last id value,
            # then return the id of the latest document
            if last <= self.last:
                return Results(self, last=last, runtime=0.0)
            # add the additional restriction that the id must be newer than 'last'.
            query = And([NumericRange('id', self.last, TailPlan._MAXID, True), self.query]).normalize()
            # query each index, and aggregate the results
            rlist = []
            runtime = 0.0
            last = 0
            for i in self.indices:
                result = i.search(query, self.limit, self.sorting, self.reverse)
                rlist.append(result)
                runtime += result.runtime
                try:
                    _last = result.docnum(-1)
                    if _last > last: last = _last
                except IndexError:
                    # if there are no results, result.docnum() raises IndexError
                    pass
            return Results(self, *rlist, runtime=runtime, last=last)
        except Exception, e:
            raise PlanExecutionError(str(e))

class ListIndicesPlan(ExecutionPlan):
    def __init__(self):
        self.indices = tuple(db._indices.keys())

    def execute(self):
        return Results(self, [{'index':{'value':v}} for v in self.indices])

class ShowIndexPlan(ExecutionPlan):
    def __init__(self, name):
        self.name = name
        try:
            self.index = db._indices[name]
        except KeyError, e:
            raise PlanConfigurationError("unknown index '%s'" % e)

    def execute(self):
        meta = {}
        meta['name'] = self.name
        meta['size'] = self.index.doc_count()
        meta['last-modified'] = self.index.last_modified()
        meta['last-id'] = self.index.last_id()
        schema = [{'field':{'value':name}} for name in self.index.schema.names()]
        return Results(self, schema, **meta)

class Results(object):
    def __init__(self, plan, *results, **meta):
        logger.trace("Results.__init__(): collating search results")
        self._plan = plan
        # ugly hack: we wrap meta in a tuple, so we can differentiate the first
        # row (the meta row) from the rows of results
        self._results = []
        for r in results:
            self._results.extend(list(r))
        if len(results) > 1 and self._plan.sorting != None:
            def keyfn(item):
                if len(self._plan.sorting) == 1:
                    if self._plan.sorting[0] in item:
                        return item[self._plan.sorting[0]]['value']
                    return ()
                return [item[k]['value'] for k in self._plan.sorting if k in item]
            self._results.sort(key=keyfn, reverse=self._plan.reverse)
        # ugly hack: we wrap meta in a tuple, so we can differentiate the first
        # row (the meta row) from the rows of results.  meta row is inserted at
        # the end so it is not affected by the results sorting.
        self._results.insert(0, (meta,))

    def __iter__(self):
        logger.trace("Results.__iter__(): iterating search results")
        class ResultIterator(object):
            def __init__(self, results, plan):
                self._iter = iter(results)
                self._plan = plan
            def __iter__(self):
                return self
            def next(self):
                item = self._iter.next()
                # if this is the meta row, then don't (potentially) filter on fields
                if isinstance(item, tuple):
                    return item[0]
                if not self._plan.fields == None:
                    return dict([(k,v['value']) for k,v in item.items() if k in self._plan.fields])
                return dict([(k,v['value']) for k,v in item.items()])
        return ResultIterator(self._results, self._plan)
                
    def __getitem__(self, i):
        item = self._results[i]
        # if this is the meta row, then don't (potentially) filter on fields
        if isinstance(item, tuple):
            return item[0]
        if not self._plan.fields == None:
            return dict([(k,v['value']) for k,v in item.items() if k in self._plan.fields])
        return dict([(k,v['value']) for k,v in item.items()])

    def __len__(self):
        return len(self._results)
