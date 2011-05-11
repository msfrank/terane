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

from pyparsing import ParseBaseException
from terane.dql.query import searchQuery

class ParsingSyntaxError(BaseException):
    def __init__(self, exc, qstring):
        self._exc = exc
        tokens = qstring[exc.col-1:].splitlines()[0]
        self._message = "Syntax error starting at '%s': %s (line %i, col %i)" % (
            tokens, exc.msg, exc.lineno, exc.col)
    def __str__(self):
        return self._message

def parseQuery(qstring):
    """
    Parse the query specified by qstring.  Returns a Query object.
    """
    try:
        q = searchQuery.parseString(qstring, parseAll=True).asList()
        return q[0]
    except ParseBaseException, e:
        raise ParsingSyntaxError(e, qstring)

def parseRestriction(rstring):
    """
    Parse the restriction specified by rstring.  Returns a Restriction object.
    """
    return None
