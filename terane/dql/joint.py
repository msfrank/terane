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

from whoosh.query import And as WhooshAnd
from whoosh.matching import IntersectionMatcher as WhooshIntersectionMatcher, ReadTooFar

class IntersectionMatcher(WhooshIntersectionMatcher):
    def all_ids(self):
        if not self.a.is_active() or not self.b.is_active():
            return
        curr = None
        for id in self.a.all_ids():
            if curr and id < curr:
                continue
            try:
                self.b.skip_to(id)
            except ReadTooFar:
                break
            curr = self.b.id()
            if curr == id:
                yield id

class And(WhooshAnd):
    def matcher(self, searcher):
        return self._matcher(IntersectionMatcher, None, searcher)
