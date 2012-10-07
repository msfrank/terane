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

import datetime
from pyparsing import ParseBaseException
from terane.bier.matching import Every
from terane.bier.searching import Period
from terane.bier.ql.queries import iterQuery, tailQuery
from terane.loggers import getLogger

logger = getLogger('terane.bier.ql')

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

@logger.tracedfunc
def parseIterQuery(string):
    """
    Parse the specified iter query.
    
    :param string: The query string.
    :type string: unicode
    :returns: A (query,period) tuple.
    :rtype: tuple
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
    except ParseBaseException, e:
        raise QuerySyntaxError(e, string)

@logger.tracedfunc
def parseTailQuery(string):
    """
    Parse the specified tail query.
    
    :param string: The query string.
    :type string: unicode
    :returns: A (query,period) tuple.
    :rtype: tuple
    """
    try:
        if string.strip() == '':
            return Every()
        return tailQuery.parseString(string, parseAll=True).asList()[0]
    except ParseBaseException, e:
        raise QuerySyntaxError(e, string)
