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

"""
Term Grammar
============

<alphanums>             ::= alphabetic characters 'a' through 'z', 'A' through 'Z', digits 0 through 9
<unreservedSymbols>     ::= '_' | '+' | '-' | '*' | '/' | '\' | ',' | '.' | '&' | '|' | '^' | '~' | '@' | '#' | '$' | '%' | ':' | ';'
<unreservedChars>       ::= <alphas> | <digits> | <unreservedSymbols>
<quotedString>          ::= sequence of characters starting with a single or double quotaton mark, containing a span of
                            zero or more characters, and ending with a matching single or double quotation mark
<subjectWord>           ::= <quotedString> | <unreservedChars>+
<subjectField>          ::= <unreservedChars>+
<subjectTerm>           ::= [ <subjectField> '=' ] <subjectWord>

Date Grammar
============

<dateOnly>              ::= <year> '-' <month> '-' <day>
<dateTime>              ::= <dateOnly> 'T' <hour> ':' <minute> ':' <second>
<absoluteDate>          ::= <dateTime> | <dateOnly>
<digitExcludingZero>    ::= '1'|'2'|'3'|'4'|'5'|'6'|'7'|'8'|'9'
<digit>                 ::= '0'|'1'|'2'|'3'|'4'|'5'|'6'|'7'|'8'|'9'
<positiveNumber>        ::= <digitExcludingZero> [ <digit>* ]
<timeUnit>              ::= 'SECOND' | 'SECONDS' | 'MINUTE' | 'MINUTES' | 'HOUR' | 'HOURS'
                            | 'DAY' | 'DAYS' | 'WEEK' | 'WEEKS' | 'MONTH' | 'MONTHS' | 'YEAR' | 'YEARS'
<relativeDate>          ::= <positiveNumber> <timeUnit> 'AGO'
<dateSpec>              ::= <absoluteDate> | <relativeDate>
<forwardDateRange>      ::= 'FROM' <dateSpec> [ 'EXCLUSIVE' ] [ 'TO' <dateSpec> [ 'EXCLUSIVE' ] ]
<backwardDateRange>     ::= 'TO' <dateSpec> [ 'EXCLUSIVE' ] [ 'FROM' <dateSpec> [ 'EXCLUSIVE' ] ]
<subjectDate>           ::= 'DATE' <forwardDateRange> | 'DATE' <backwardDateRange>

ID Grammar
==========

<naturalNumber>         ::= '0' | <numberExceptZero <number>*
<forwardDateRange>      ::= 'FROM' <naturalNumber> [ 'EXCLUSIVE' ] [ 'TO' <naturalNumber> [ 'EXCLUSIVE' ] ]
<backwardDateRange>     ::= 'TO' <naturalNumber> [ 'EXCLUSIVE' ] [ 'FROM' <naturalNumber> [ 'EXCLUSIVE' ] ]
<subjectId>             ::= 'ID' <forwardIDRange> | 'DATE' <backwardIDRange>

Query Grammar
=============

<searchSubject>   ::= <subjectDate> | <subjectId> | <subjectTerm>
<searchNotGroup>  ::= [ 'NOT' ] <SearchSubject> | '(' <SearchOrGroup> ')'
<searchAndGroup>  ::= <SearchSubject>  [ 'AND' <SearchNotGroup> ]*
<searchOrGroup>   ::= <SearchAndGroup> [ 'OR' <SearchAndGroup> ]*
<searchQuery>     ::= <SearchOrGroup>

<TailSubject>     ::= <subjectTerm>
<TailNotGroup>    ::= [ 'NOT' ] <TailSubject> | '(' <TailOrGroup> ')'
<TailAndGroup>    ::= <TailSubject>  [ 'AND' <TailNotGroup> ]*
<TailOrGroup>     ::= <TailAndGroup> [ 'OR' <TailAndGroup> ]*
<TailQuery>       ::= <TailOrGroup>
"""

import datetime, dateutil
import pyparsing as pp
from whoosh.query import Prefix, DateRange, NumericRange, And, Or, Not

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

def parseSearchQuery(string):
    """
    Parse the query specified by qstring.  Returns a Query object.
    """
    try:
        return searchQuery.parseString(string, parseAll=True).asList()[0]
    except pp.ParseBaseException, e:
        raise QuerySyntaxError(e, string)

def parseTailQuery(string):
    """
    Parse the query specified by qstring.  Returns a Query object.
    """
    try:
        return tailQuery.parseString(string, parseAll=True).asList()[0]
    except pp.ParseBaseException, e:
        raise QuerySyntaxError(e, string)

# subjectTerm
unreservedChars = pp.alphanums + '_+-*/\,.&|^~@#$%:;'
fieldSeparator = pp.Suppress('=')
subjectWord = pp.quotedString | pp.Word(unreservedChars)
subjectTerm = pp.Optional(pp.Word(unreservedChars) + fieldSeparator) + subjectWord
def parseSubjectTerm(tokens):
    if len(tokens) == 1:
        return Prefix('default', unicode(tokens[0]))
    return Prefix(tokens[0], unicode(tokens[1]))
subjectTerm.setParseAction(parseSubjectTerm)

def makeUTC(dt):
    # if no timezone is specified, then assume local tz
    if dt.tzinfo == None:
        dt = dt.replace(tzinfo=dateutil.tz.tzlocal())
    # convert to UTC, if necessary
    if not dt.tzinfo == dateutil.tz.tzutc():
        dt = dt.astimezone(dateutil.tz.tzutc())
    # return a timezone-naive datetime object for Whoosh
    return dt.replace(tzinfo=None) - dt.utcoffset()

# absoluteDate
dateOnly = pp.Combine(pp.Word(pp.nums) + '/' + pp.Word(pp.nums) + '/' + pp.Word(pp.nums))
dateTime = dateOnly + pp.Combine('T' + pp.Word(pp.nums) + ':' + pp.Word(pp.nums) + ':' + pp.Word(pp.nums))
absoluteDate = dateTime | dateOnly
def parseAbsoluteDate(pstr, loc, tokens):
    try:
        if len(tokens) > 1:
            return makeUTC(datetime.datetime.strptime(tokens[0] + tokens[1], "%Y/%m/%dT%H:%M:%S"))
        return makeUTC(datetime.datetime.strptime(tokens[0], "%Y/%m/%d"))
    except ValueError, e:
        raise ParseFatalException(pstr, loc, "Invalid date specification")
absoluteDate.setParseAction(parseAbsoluteDate)

# relativeDate
positiveNumber = pp.Word(pp.srange('[1-9]'), pp.nums)
secondsAgo = positiveNumber + pp.Suppress(pp.oneOf('SECONDS SECOND'))
secondsAgo.setParseAction(lambda tokens: datetime.timedelta(seconds=int(tokens[0])))
minutesAgo = positiveNumber + pp.Suppress(pp.oneOf('MINUTES MINUTE'))
minutesAgo.setParseAction(lambda tokens: datetime.timedelta(minutes=int(tokens[0])))
hoursAgo = positiveNumber + pp.Suppress(pp.oneOf('HOURS HOUR'))
hoursAgo.setParseAction(lambda tokens: datetime.timedelta(hours=int(tokens[0])))
daysAgo = positiveNumber + pp.Suppress(pp.oneOf('DAYS DAY'))
daysAgo.setParseAction(lambda tokens: datetime.timedelta(days=int(tokens[0])))
weeksAgo = positiveNumber + pp.Suppress(pp.oneOf('WEEKS WEEK'))
weeksAgo.setParseAction(lambda tokens: datetime.timedelta(weeks=int(tokens[0])))
relativeDate = ( secondsAgo | minutesAgo | hoursAgo | daysAgo | weeksAgo ) + pp.Suppress('AGO')
relativeDate.setParseAction(lambda tokens: makeUTC(datetime.datetime.now()) - tokens[0])

# subjectDate
dateSpec = relativeDate | absoluteDate
optionalExclusive = pp.Optional(pp.Literal('EXCLUSIVE'))
dateFrom = pp.Suppress('FROM') + dateSpec + optionalExclusive
def parseDateFrom(tokens):
    date = {'from': tokens[0]}
    if len(tokens) > 1:
        date['fromExcl'] = True
    return date
dateFrom.setParseAction(parseDateFrom)
dateTo = pp.Suppress('TO') + dateSpec + optionalExclusive
def parseDateTo(tokens):
    date = {'to': tokens[0]}
    if len(tokens) > 1:
        date['toExcl'] = True
    return date
dateTo.setParseAction(parseDateTo)
subjectDate = pp.Suppress('DATE') + dateFrom + pp.Optional(dateTo) | pp.Suppress('DATE') + dateTo + pp.Optional(dateFrom) 
def parseSubjectDate(tokens):

    date = {'from': makeUTC(datetime.datetime.min), 'to': makeUTC(datetime.datetime.now()), 'fromExcl': False, 'toExcl': False}
    date.update(tokens[0])
    if len(tokens) > 1:
        date.update(tokens[1])
    return DateRange('ts', date['from'], date['to'], startexcl=date['fromExcl'], endexcl=date['toExcl'])
subjectDate.setParseAction(parseSubjectDate)

# subjectId
naturalNumber = '0' | pp.Word(pp.srange('[1-9]'), pp.nums)
optionalExclusive = pp.Optional(pp.Literal('EXCLUSIVE'))
idFrom = pp.Suppress('FROM') + naturalNumber + optionalExclusive
def parseIDFrom(pstr, loc, tokens):
    try:
        docid = {'from': long(tokens[0])}
    except ValueError:
        raise ParseFatalException(pstr, loc, "Invalid document ID")
    if len(tokens) > 1:
        docid['fromExcl'] = True
    return docid
idFrom.setParseAction(parseIDFrom)
idTo = pp.Suppress('TO') + naturalNumber + optionalExclusive
def parseIDTo(pstr, loc, tokens):
    try:
        docid = {'to': long(tokens[0])}
    except ValueError:
        raise ParseFatalException(pstr, loc, "Invalid document ID")
    if len(tokens) > 1:
        docid['toExcl'] = True
    return docid
idTo.setParseAction(parseIDTo)
subjectId = pp.Suppress('ID') + idFrom + pp.Optional(idTo) | pp.Suppress('ID') + idTo + pp.Optional(idFrom) 
def parseSubjectId(tokens):
    docid = {'from': 0, 'to': 2**64, 'fromExcl': False, 'toExcl': False}
    docid.update(tokens[0])
    if len(tokens) > 1:
        docid.update(tokens[1])
    return NumericRange('id', docid['from'], docid['to'], startexcl=docid['fromExcl'], endexcl=docid['toExcl'])
subjectId.setParseAction(parseSubjectId)

# groupings
def parseNotGroup(tokens):
    if len(tokens) == 1:
        q = tokens[0]
    else:
        q = Not(tokens[1])
    return q
def parseAndGroup(tokens):
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
def parseOrGroup(tokens):
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

# searchQuery
searchSubject = subjectDate | subjectId | subjectTerm
searchQuery = pp.operatorPrecedence(subjectDate | subjectId | subjectTerm, [
    (pp.Suppress('NOT'), 1, pp.opAssoc.RIGHT, parseNotGroup),
    (pp.Suppress('AND'), 2, pp.opAssoc.LEFT, parseAndGroup),
    (pp.Suppress('OR'), 2, pp.opAssoc.LEFT, parseOrGroup),
    ])

# tailQuery
tailQuery = pp.operatorPrecedence(subjectTerm, [
    (pp.Suppress('NOT'), 1, pp.opAssoc.RIGHT, parseNotGroup),
    (pp.Suppress('AND'), 2, pp.opAssoc.LEFT, parseAndGroup),
    (pp.Suppress('OR'), 2, pp.opAssoc.LEFT, parseOrGroup),
    ])
