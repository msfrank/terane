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

<IterSubject>     ::= <subjectTerm>
<IterNotGroup>    ::= [ 'NOT' ] <IterSubject> | '(' <IterOrGroup> ')'
<IterAndGroup>    ::= <IterSubject>  [ 'AND' <IterNotGroup> ]*
<IterOrGroup>     ::= <IterAndGroup> [ 'OR' <IterAndGroup> ]*
<IterQuery>       ::= <IterOrGroup> [ 'WHERE' <subjectDate> ]

<TailSubject>     ::= <subjectTerm>
<TailNotGroup>    ::= [ 'NOT' ] <TailSubject> | '(' <TailOrGroup> ')'
<TailAndGroup>    ::= <TailSubject>  [ 'AND' <TailNotGroup> ]*
<TailOrGroup>     ::= <TailAndGroup> [ 'OR' <TailAndGroup> ]*
<TailQuery>       ::= <TailOrGroup>
"""

import datetime, dateutil.tz
import pyparsing as pp
from terane.bier.searching import Term, Period, AND, OR, NOT

class QuerySyntaxError(BaseException):
    """
    There was an error parsing the query synatx.
    """
    def __init__(self, exc, qstring):
        self._exc = exc
        try:
            tokens = qstring[exc.col-1:].splitlines()[0]
        except:
            tokens = ''
        if exc.msg != '':
            self._message = "Syntax error starting at '%s' (line %i, col %i): %s" % (
                tokens, exc.lineno, exc.col, exc.msg)
        else:
            self._message = "Syntax error starting at '%s' (line %i, col %i)" % (
                tokens, exc.lineno, exc.col)
    def __str__(self):
        return self._message

def parseIterQuery(string):
    """
    Parse the iter query specified by qstring.  Returns a Query object.
    """
    try:
        if string.strip() == '':
            query,where = Every(), None
        else:
            query,where = iterQuery.parseString(string, parseAll=True).asList()[0]
        if where == None:
            utcnow = datetime.datetime.utcnow()
            onehourago = utcnow - datetime.timedelta(hours=1)
            where = {'dateFrom': onehourago, 'dateTo': utcnow, 'fromExcl': False, 'toExcl': False}
        return query, Period(where['dateFrom'], where['dateTo'], where['fromExcl'], where['toExcl'])
    except pp.ParseBaseException, e:
        raise QuerySyntaxError(e, string)

def parseTailQuery(string):
    "Parse the tail query specified by qstring.  Returns a Query object."
    try:
        return tailQuery.parseString(string, parseAll=True).asList()[0]
    except pp.ParseBaseException, e:
        raise QuerySyntaxError(e, string)

# subjectTerm
unreservedChars = pp.alphanums + '_+-*/\,.&|^~@#$%:;'
fieldSeparator = pp.Suppress('=')
subjectWord = pp.quotedString | pp.Word(unreservedChars)
subjectTerm = ( pp.Word(unreservedChars) + fieldSeparator + subjectWord ) | subjectWord
def parseSubjectTerm(tokens):
    "Parse a subject term."
    if len(tokens) == 1:
        return Term(None, unicode(tokens[0]))
    return Term(str(tokens[0]), unicode(tokens[1]))
subjectTerm.setParseAction(parseSubjectTerm)

def makeUTC(dt):
    "Convert the supplied datetime object to UTC, if necessary."
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
    "Parse an absolute date or datetime."
    try:
        if len(tokens) > 1:
            return makeUTC(datetime.datetime.strptime(tokens[0] + tokens[1], "%Y/%m/%dT%H:%M:%S"))
        return makeUTC(datetime.datetime.strptime(tokens[0], "%Y/%m/%d"))
    except ValueError, e:
        raise ParseFatalException(pstr, loc, "Invalid DATE specification")
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
    "Parse DATE lower bound."
    date = {'dateFrom': tokens[0]}
    if len(tokens) > 1:
        date['fromExcl'] = True
    return date
dateFrom.setParseAction(parseDateFrom)
dateTo = pp.Suppress('TO') + dateSpec + optionalExclusive
def parseDateTo(tokens):
    "Parse DATE upper bound."
    date = {'dateTo': tokens[0]}
    if len(tokens) > 1:
        date['toExcl'] = True
    return date
dateTo.setParseAction(parseDateTo)
subjectDate = pp.Suppress('DATE') + dateFrom + pp.Optional(dateTo) | pp.Suppress('DATE') + dateTo + pp.Optional(dateFrom) 
def parseSubjectDate(tokens):
    "Parse DATE range."
    date = {
        'dateFrom': datetime.datetime.min,
        'dateTo': makeUTC(datetime.datetime.now()),
        'fromExcl': False,
        'toExcl': False
        }
    date.update(tokens[0])
    if len(tokens) > 1:
        date.update(tokens[1])
    return date
subjectDate.setParseAction(parseSubjectDate)

# groupings
def parseNotGroup(tokens):
    "Parse NOT statement."
    return NOT(tokens[0][0])

def parseAndGroup(tokens):
    "Parse AND statement."
    tokens = tokens[0]
    q = tokens[0]
    if len(tokens) > 1:
        i = 1
        while i < len(tokens):
            q = AND([q, tokens[i]])
            i += 1
    return q

def parseOrGroup(tokens):
    "Parse OR statement."
    tokens = tokens[0]
    q = tokens[0]
    if len(tokens) > 1:
        i = 1
        while i < len(tokens):
            q = OR([q, tokens[i]])
            i += 1
    return q

# iterQuery
iterTerms = pp.operatorPrecedence(subjectTerm, [
    (pp.Suppress('NOT'), 1, pp.opAssoc.RIGHT, parseNotGroup),
    (pp.Suppress('AND'), 2, pp.opAssoc.LEFT,  parseAndGroup),
    (pp.Suppress('OR'),  2, pp.opAssoc.LEFT,  parseOrGroup),
    ])
iterTermsAndWhere = iterTerms + pp.Optional((pp.Suppress('WHERE') + subjectDate))
def parseIterTermsAndWhere(tokens):
    if len(tokens) > 1:
        return tokens[0], tokens[1]
    return tokens[0], None
iterTermsAndWhere.setParseAction(parseIterTermsAndWhere)
iterWhereOnly = pp.Suppress('WHERE') + subjectDate
iterWhereOnly.setParseAction(lambda t: (Every(), t[0]))
iterQuery = iterWhereOnly | iterTermsAndWhere

# tailQuery
tailQuery = pp.operatorPrecedence(subjectTerm, [
    (pp.Suppress('NOT'), 1, pp.opAssoc.RIGHT, parseNotGroup),
    (pp.Suppress('AND'), 2, pp.opAssoc.LEFT,  parseAndGroup),
    (pp.Suppress('OR'),  2, pp.opAssoc.LEFT,  parseOrGroup),
    ])
