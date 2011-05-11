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

from pyparsing import srange, nums, Word, Literal, Suppress, Optional, ParseFatalException
from whoosh.query import NumericRange

"""
Terane Query Language ID grammar
====================================

<naturalNumber>         ::= '0' | <numberExceptZero <number>*
<forwardDateRange>      ::= 'FROM' <naturalNumber> [ 'EXCLUSIVE' ] [ 'TO' <naturalNumber> [ 'EXCLUSIVE' ] ]
<backwardDateRange>     ::= 'TO' <naturalNumber> [ 'EXCLUSIVE' ] [ 'FROM' <naturalNumber> [ 'EXCLUSIVE' ] ]
<searchID>              ::= 'ID' <forwardIDRange> | 'DATE' <backwardIDRange>
"""


naturalNumber = '0' | Word(srange('[1-9]'), nums)
optionalExclusive = Optional(Literal('EXCLUSIVE'))

idFrom = Suppress('FROM') + naturalNumber + optionalExclusive
def parseIDFrom(pstr, loc, tokens):
    try:
        docid = {'from': long(tokens[0])}
    except ValueError:
        raise ParseFatalException(pstr, loc, "Invalid document ID")
    if len(tokens) > 1:
        docid['fromExcl'] = True
    return docid
idFrom.setParseAction(parseIDFrom)

idTo = Suppress('TO') + naturalNumber + optionalExclusive
def parseIDTo(pstr, loc, tokens):
    try:
        docid = {'to': long(tokens[0])}
    except ValueError:
        raise ParseFatalException(pstr, loc, "Invalid document ID")
    if len(tokens) > 1:
        docid['toExcl'] = True
    return docid
idTo.setParseAction(parseIDTo)

searchID = Suppress('ID') + idFrom + Optional(idTo) | Suppress('ID') + idTo + Optional(idFrom) 
def parseSearchID(tokens):
    docid = {'from': 0, 'to': 4294967295, 'fromExcl': False, 'toExcl': False}
    docid.update(tokens[0])
    if len(tokens) > 1:
        docid.update(tokens[1])
    return NumericRange('id', docid['from'], docid['to'], startexcl=docid['fromExcl'], endexcl=docid['toExcl'])
searchID.setParseAction(parseSearchID)
