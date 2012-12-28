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

import pyparsing as pp
from terane.bier.matching import Every, AND, OR, NOT
from terane.bier.ql.term import subjectTerm
from terane.bier.ql.date import subjectDate
from terane.loggers import getLogger

logger = getLogger('terane.bier.ql.queries')

def _parseNotGroup(tokens):
    return NOT(tokens[0][0])

def _parseAndGroup(tokens):
    tokens = tokens[0]
    q = tokens[0]
    if len(tokens) > 1:
        i = 1
        while i < len(tokens):
            q = AND([q, tokens[i]])
            i += 1
    return q

def _parseOrGroup(tokens):
    tokens = tokens[0]
    q = tokens[0]
    if len(tokens) > 1:
        i = 1
        while i < len(tokens):
            q = OR([q, tokens[i]])
            i += 1
    return q

allTerms = pp.Suppress('ALL')
def _parseAllTerms(tokens):
    return Every()
allTerms.setParseAction(_parseAllTerms)

# -----------------
# iterQuery grammar
# -----------------
#  <IterSubject>     ::= <subjectTerm>
#  <IterNotGroup>    ::= [ 'NOT' ] <IterSubject> | '(' <IterOrGroup> ')'
#  <IterAndGroup>    ::= <IterSubject>  [ 'AND' <IterNotGroup> ]*
#  <IterOrGroup>     ::= <IterAndGroup> [ 'OR' <IterAndGroup> ]*
#  <IterQuery>       ::= ( 'ALL' | <IterOrGroup> ) [ 'WHERE' <subjectDate> ]

iterTerms = allTerms | pp.operatorPrecedence(subjectTerm, [
    (pp.Suppress('NOT'), 1, pp.opAssoc.RIGHT, _parseNotGroup),
    (pp.Suppress('AND'), 2, pp.opAssoc.LEFT,  _parseAndGroup),
    (pp.Suppress('OR'),  2, pp.opAssoc.LEFT,  _parseOrGroup),
    ])
iterQuery = iterTerms + pp.Optional((pp.Suppress('WHERE') + subjectDate))
def _parseIterQuery(tokens):
    if len(tokens) > 1:
        return tokens[0], tokens[1]
    return tokens[0], None
iterQuery.setParseAction(_parseIterQuery)

# -----------------
# tailQuery grammar
# -----------------
#  <TailSubject>     ::= <subjectTerm>
#  <TailNotGroup>    ::= [ 'NOT' ] <TailSubject> | '(' <TailOrGroup> ')'
#  <TailAndGroup>    ::= <TailSubject>  [ 'AND' <TailNotGroup> ]*
#  <TailOrGroup>     ::= <TailAndGroup> [ 'OR' <TailAndGroup> ]*
#  <TailQuery>       ::= <TailOrGroup>

tailQuery = allTerms | pp.operatorPrecedence(subjectTerm, [
    (pp.Suppress('NOT'), 1, pp.opAssoc.RIGHT, _parseNotGroup),
    (pp.Suppress('AND'), 2, pp.opAssoc.LEFT,  _parseAndGroup),
    (pp.Suppress('OR'),  2, pp.opAssoc.LEFT,  _parseOrGroup),
    ])
