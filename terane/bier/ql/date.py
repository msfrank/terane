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

import datetime, dateutil.tz
import pyparsing as pp
from terane.loggers import getLogger

logger = getLogger('terane.bier.ql.date')

def _makeUTC(dt):
    "Convert the supplied datetime object to UTC, if necessary."
    # if no timezone is specified, then assume local tz
    if dt.tzinfo == None:
        dt = dt.replace(tzinfo=dateutil.tz.tzlocal())
    # convert to UTC, if necessary
    if not dt.tzinfo == dateutil.tz.tzutc():
        dt = dt.astimezone(dateutil.tz.tzutc())
    # return a timezone-naive datetime object for Whoosh
    return dt.replace(tzinfo=None) - dt.utcoffset()

# --------------------
# absoluteDate grammar
# --------------------
#  <year>                  ::= <digit>*4
#  <month>                 ::= <digit>*2
#  <day>                   ::= <digit>*2
#  <hour>                  ::= <digit>*2
#  <minute>                ::= <digit>*2
#  <second>                ::= <digit>*2
#  <dateOnly>              ::= <year> '-' <month> '-' <day>
#  <dateTime>              ::= <dateOnly> 'T' <hour> ':' <minute> ':' <second>
#  <absoluteDate>          ::= <dateTime> | <dateOnly>

dateOnly = pp.Combine(pp.Word(pp.nums) + '/' + pp.Word(pp.nums) + '/' + pp.Word(pp.nums))
dateTime = dateOnly + pp.Combine('T' + pp.Word(pp.nums) + ':' + pp.Word(pp.nums) + ':' + pp.Word(pp.nums))
absoluteDate = dateTime | dateOnly
def parseAbsoluteDate(pstr, loc, tokens):
    "Parse an absolute date or datetime."
    try:
        if len(tokens) > 1:
            return _makeUTC(datetime.datetime.strptime(tokens[0] + tokens[1], "%Y/%m/%dT%H:%M:%S"))
        return _makeUTC(datetime.datetime.strptime(tokens[0], "%Y/%m/%d"))
    except ValueError, e:
        raise ParseFatalException(pstr, loc, "Invalid DATE specification")
absoluteDate.setParseAction(parseAbsoluteDate)

# --------------------
# relativeDate grammar
# --------------------
# <digitExcludingZero>    ::= '1'|'2'|'3'|'4'|'5'|'6'|'7'|'8'|'9'
# <digit>                 ::= '0'|'1'|'2'|'3'|'4'|'5'|'6'|'7'|'8'|'9'
# <positiveNumber>        ::= <digitExcludingZero> [ <digit>* ]
# <timeUnit>              ::= 'SECOND' | 'SECONDS' | 'MINUTE' | 'MINUTES' | 'HOUR' | 'HOURS'
#                             | 'DAY' | 'DAYS' | 'WEEK' | 'WEEKS' | 'MONTH' | 'MONTHS' | 'YEAR' | 'YEARS'
# <relativeDate>          ::= <positiveNumber> <timeUnit> 'AGO'

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
relativeDate.setParseAction(lambda tokens: _makeUTC(datetime.datetime.now()) - tokens[0])

# -------------------
# subjectDate grammar
# -------------------
# <dateSpec>              ::= <absoluteDate> | <relativeDate>
# <forwardDateRange>      ::= 'FROM' <dateSpec> [ 'EXCLUSIVE' ] [ 'TO' <dateSpec> [ 'EXCLUSIVE' ] ]
# <backwardDateRange>     ::= 'TO' <dateSpec> [ 'EXCLUSIVE' ] [ 'FROM' <dateSpec> [ 'EXCLUSIVE' ] ]
# <subjectDate>           ::= 'DATE' <forwardDateRange> | 'DATE' <backwardDateRange>

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
        'dateTo': _makeUTC(datetime.datetime.now()),
        'fromExcl': False,
        'toExcl': False
        }
    date.update(tokens[0])
    if len(tokens) > 1:
        date.update(tokens[1])
    return date
subjectDate.setParseAction(parseSubjectDate)
