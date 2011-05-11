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

from pyparsing import Suppress, Optional, operatorPrecedence, opAssoc
from whoosh.query import Or, Not
from terane.dql.term import searchTerm
from terane.dql.id import searchID
from terane.dql.date import searchDate
from terane.dql.joint import And

"""
Terane Query Language query grammar
===================================

<searchSubject>         ::= <searchDate> | <searchID> | <searchTerm>
<searchNotGroup>        ::= [ 'NOT' ] <searchTerm> | '(' <searchOrGroup> ')'
<searchAndGroup>        ::= <searchTerm>  [ 'AND' <searchNotGroup> ]*
<searchOrGroup>         ::= <searchAndGroup> [ 'OR' <searchAndGroup> ]*
<searchQuery>           ::= <searchOrGroup> [ <restriction> ]
"""

searchSubject = searchDate | searchID | searchTerm

def parseSearchNotGroup(tokens):
    if len(tokens) == 1:
        q = tokens[0]
    else:
        q = Not(tokens[1])
    return q

def parseSearchAndGroup(tokens):
    tokens = tokens[0]
    if len(tokens) == 1:
        q = tokens[0]
    else:
        q = tokens[0]
        i = 1
        while i < len(tokens):
            q = And([q, tokens[i]]).normalize()
            i += 1
    return q

def parseSearchOrGroup(tokens):
    tokens = tokens[0]
    if len(tokens) == 1:
        q = tokens[0]
    else:
        q = tokens[0]
        i = 1
        while i < len(tokens):
            q = Or([q, tokens[i]]).normalize()
            i += 1
    return q

searchQuery = operatorPrecedence(searchSubject, [
    (Suppress('NOT'), 1, opAssoc.RIGHT, parseSearchNotGroup),
    (Suppress('AND'), 2, opAssoc.LEFT, parseSearchAndGroup),
    (Suppress('OR'), 2, opAssoc.LEFT, parseSearchOrGroup),
    ])
