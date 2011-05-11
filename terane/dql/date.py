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

import datetime
from pyparsing import srange, nums, Word, oneOf, Literal, Suppress, Optional, Combine, ParseFatalException
from whoosh.query import DateRange

"""
Terane Query Language date grammar
====================================

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
<searchDate>            ::= 'DATE' <forwardDateRange> | 'DATE' <backwardDateRange>
"""

dateOnly = Combine(Word(nums) + '/' + Word(nums) + '/' + Word(nums))
dateTime = dateOnly + Combine('T' + Word(nums) + ':' + Word(nums) + ':' + Word(nums))
absoluteDate = dateTime | dateOnly
def parseAbsoluteDate(pstr, loc, tokens):
    try:
        if len(tokens) > 1:
            return datetime.datetime.strptime(tokens[0] + tokens[1], "%Y/%m/%dT%H:%M:%S")
        return datetime.datetime.strptime(tokens[0], "%Y/%m/%d")
    except ValueError, e:
        raise ParseFatalException(pstr, loc, "Invalid date specification")
absoluteDate.setParseAction(parseAbsoluteDate)

positiveNumber = Word(srange('[1-9]'), nums)

secondsAgo = positiveNumber + Suppress(oneOf('SECONDS SECOND'))
secondsAgo.setParseAction(lambda tokens: datetime.timedelta(seconds=int(tokens[0])))
minutesAgo = positiveNumber + Suppress(oneOf('MINUTES MINUTE'))
minutesAgo.setParseAction(lambda tokens: datetime.timedelta(minutes=int(tokens[0])))
hoursAgo = positiveNumber + Suppress(oneOf('HOURS HOUR'))
hoursAgo.setParseAction(lambda tokens: datetime.timedelta(hours=int(tokens[0])))
daysAgo = positiveNumber + Suppress(oneOf('DAYS DAY'))
daysAgo.setParseAction(lambda tokens: datetime.timedelta(days=int(tokens[0])))
weeksAgo = positiveNumber + Suppress(oneOf('WEEKS WEEK'))
weeksAgo.setParseAction(lambda tokens: datetime.timedelta(weeks=int(tokens[0])))

relativeDate = ( secondsAgo | minutesAgo | hoursAgo | daysAgo | weeksAgo ) + Suppress('AGO')
relativeDate.setParseAction(lambda tokens: datetime.datetime.now() - tokens[0])

dateSpec = relativeDate | absoluteDate

optionalExclusive = Optional(Literal('EXCLUSIVE'))

dateFrom = Suppress('FROM') + dateSpec + optionalExclusive
def parseDateFrom(tokens):
    date = {'from': tokens[0]}
    if len(tokens) > 1:
        date['fromExcl'] = True
    return date
dateFrom.setParseAction(parseDateFrom)

dateTo = Suppress('TO') + dateSpec + optionalExclusive
def parseDateTo(tokens):
    date = {'to': tokens[0]}
    if len(tokens) > 1:
        date['toExcl'] = True
    return date
dateTo.setParseAction(parseDateTo)

searchDate = Suppress('DATE') + dateFrom + Optional(dateTo) | Suppress('DATE') + dateTo + Optional(dateFrom) 
def parseSearchDate(tokens):
    date = {'from': datetime.datetime.min, 'to': datetime.datetime.now(), 'fromExcl': False, 'toExcl': False}
    date.update(tokens[0])
    if len(tokens) > 1:
        date.update(tokens[1])
    return DateRange('ts', date['from'], date['to'], startexcl=date['fromExcl'], endexcl=date['toExcl'])
searchDate.setParseAction(parseSearchDate)
