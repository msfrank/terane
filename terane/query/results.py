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

from terane.loggers import getLogger

logger = getLogger('terane.query.results')

class Results(object):
    def __init__(self, sorting, fields, reverse):
        self.sorting = sorting
        self.fields = fields
        self.reverse = reverse
        self._results = []

    def extend(self, *results, **meta):
        for r in results:
            self._results.extend(list(r))
        if len(results) > 1 and self.sorting != None:
            def keyfn(item):
                if len(self.sorting) == 1:
                    if self.sorting[0] in item:
                        return item[self.sorting[0]]['value']
                    return ()
                return [item[k]['value'] for k in self.sorting if k in item]
            self._results.sort(key=keyfn, reverse=self.reverse)
        # ugly hack: we wrap meta in a tuple, so we can differentiate the first
        # row (the meta row) from the rows of results.  meta row is inserted at
        # the end so it is not affected by the results sorting.
        self._results.insert(0, (meta,))

    def __iter__(self):
        return ResultIterator(iter(self._results), self.fields)
                
    def __getitem__(self, i):
        item = self._results[i]
        # if this is the meta row, then don't (potentially) filter on fields
        if isinstance(item, tuple):
            return item[0]
        if not self._fields == None:
            return dict([(k,v['value']) for k,v in item.items() if k in self.fields])
        return dict([(k,v['value']) for k,v in item.items()])

    def __len__(self):
        return len(self._results)

class ResultIterator(object):
    """
    """

    def __init__(self, results, fields):
        self._results = results
        self._fields = fields

    def __iter__(self):
        return self

    def next(self):
        item = self._results.next()
        # if this is the meta row, then don't (potentially) filter on fields
        if isinstance(item, tuple):
            return item[0]
        if not self._fields == None:
            return dict([(k,v['value']) for k,v in item.items() if k in self._fields])
        return dict([(k,v['value']) for k,v in item.items()])
