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

import datetime, calendar, struct, base64, re
from terane.loggers import getLogger

logger = getLogger('terane.bier.schema')

class IdentityField(object):
    """
    IdentityField stores data as-is, without any linguistic processing.
    """

    def terms(self, value):
        """
        The IdentityField tokenizer just converts each item in value to unicode,
        if necessary.

        :param value: The list or tuple value to tokenize.
        :type value: list or tuple
        :returns: A list of tokenized terms.
        :rtype: list
        """
        if not isinstance(value, list) and not isinstance(value, tuple):
            raise Exception("value '%s' is not of type list or tuple" % value)
        return [unicode(t) for t in value if t != '']

    def parse(self, value):
        """
        Return a list of tuples, each containing a tokenized term and a dict
        containing term metadata.  The metadata for an IdentityField term is the
        'pos' item which is a list of term positions.

        :param value: The list or tuple value to parse.
        :type value: list or tuple
        :returns: A list of (term, metadata) tuples.
        :rtype: list
        """
        terms = self.terms(value)
        positions = {}
        for position in range(len(terms)):
            term = terms[position]
            if term in positions:
                positions[term]['pos'].append(position)
            else:
                positions[term] = {'pos': [position]}
        return positions.items()

class TextField(object):
    """
    TextField stores input by breaking it into tokens separated by whitespace or
    any character other than an underscore, numeral, or alphanumeric character
    (as designated by the unicode properties database).  Tokens in a TextField are
    always lowercased, so queries on a TextField are case-insensitive.
    """

    def terms(self, value):
        """
        Process the specified unicode or string value, breaking it up into tokens.
        Tokens are separated by any run of one or more characters which are not in
        [0-9_] and not alphanumeric as defined by the unicode properties database.
        Further, each token is converted to all lowercase if necessary.

        :param value: The string value to tokenize.
        :type value: unicode or str
        :returns: A list of tokenized terms.
        :rtype: list
        """
        if not isinstance(value, unicode) and not isinstance(value, str):
            raise Exception("value '%s' is not of type unicode or str" % value)
        return [unicode(t.lower()) for t in re.split(r'\W+', value, re.UNICODE) if t != '']

    def parse(self, value):
        """
        Process the specified unicode or string value, breaking it up into tokens,
        and return a list of tuples, each containing a tokenized term and a dict
        containing term metadata.  The metadata for a TextField term is the 'pos'
        item which is a list of term positions.

        :param value: The string value to parse.
        :type value: unicode or str
        :returns: A list of (term, metadata) tuples.
        :rtype: list
        """
        terms = self.terms(value)
        positions = {}
        for position in range(len(terms)):
            term = terms[position]
            if term in positions:
                positions[term]['pos'].append(position)
            else:
                positions[term] = {'pos': [position]}
        return positions.items()

class DatetimeField(object):
    """
    DatetimeField stores a python datetime.datetime.
    """

    def terms(self, value):
        if not isinstance(value, datetime.datetime):
            raise Exception("value '%s' is not of type datetime.datetime" % value)
        # calculate the unix timestamp, with 1 second accuracy
        ts = int(calendar.timegm(value.timetuple()))
        # pack the int as a 32 bit big-endian integer
        ts = struct.pack(">I", ts)
        # convert the packed int to base64   
        ts = unicode(base64.standard_b64encode(ts))
        return [ts,]

    def parse(self, value):
        return [(self.terms(value)[0], None)]

def fieldFactory(value):
    """
    Given a value, return a new field instance of the appropriate type for the value.

    :param value: The object to instantiate a new field for.
    :type value: object
    :returns: A new field instance.
    :rtype: An object implementing :class:`terane.bier.IField`
    """
    if isinstance(value, str) or isinstance(value, unicode):
        return TextField()
    if isinstance(value, datetime.datetime):
        return DatetimeField()
    if isinstance(value, list) or isinstance(value, tuple):
        return IdentityField()
    raise TypeError("unknown event value type '%s'" % str(type(value)))
