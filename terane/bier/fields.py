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

import datetime, calendar, dateutil.tz, re
from zope.interface import implements
from terane.bier import IField
from terane.loggers import getLogger

logger = getLogger('terane.bier.schema')

class IdentityField(object):
    """
    IdentityField stores data as-is, without any linguistic processing.
    """

    implements(IField)

    def validate(self, value):
        """
        Validate that the supplied value is a string (str or unicode).

        :param value: The value to validate.
        :type value: str or unicode
        :returns: The value as a unicode object.
        :rtype: unicode
        :raises TypeError: The value if of the wrong type.
        """
        if not isinstance(value, unicode) and not isinstance(value, str):
            raise TypeError('value must be of type unicode or str')
        return unicode(value)

    def terms(self, value):
        """
        The IdentityField tokenizer just converts each item in value to unicode,
        if necessary.

        :param value: The list or tuple value to tokenize.
        :type value: list or tuple
        :returns: A list of tokenized terms.
        :rtype: list
        """
        if not isinstance(value, unicode) and not isinstance(value, str):
            raise Exception("value '%s' is not of type unicode or str" % value)
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

    implements(IField)

    def validate(self, value):
        """
        Validate that the supplied value is a string (str or unicode).

        :param value: The value to validate.
        :type value: str or unicode
        :returns: The value as a unicode object.
        :rtype: unicode
        :raises TypeError: The value if of the wrong type.
        """
        if not isinstance(value, unicode) and not isinstance(value, str):
            raise TypeError('value must be of type unicode or str')
        return unicode(value)

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

    implements(IField)

    def validate(self, value):
        """
        Validate that the supplied value is a datetime.datetime, and convert
        the value to UTC timezone if necessary.

        :param value: The value to validate.
        :type value: datetime.datetime
        :returns: The value
        :rtype: datetime.datetime
        :raises TypeError: The value if of the wrong type.
        """
        if not isinstance(value, datetime.datetime):
            raise TypeError('value must be of type datetime.datetime')
        # if no timezone is specified, then assume local tz
        if value.tzinfo == None:
            value = value.replace(tzinfo=dateutil.tz.tzlocal())
        # convert to UTC, if necessary
        if not value.tzinfo == dateutil.tz.tzutc():
            value = value.astimezone(dateutil.tz.tzutc())
        return value

    def terms(self, value):
        """
        Process the specified datetime.datetime value, converting it to a unix
        timestamp.

        :param value: The string value to tokenize.
        :type value: unicode or str
        :returns: A list of tokenized terms.
        :rtype: list
        """
        # calculate the unix timestamp, with 1 second accuracy
        ts = int(calendar.timegm(value.timetuple()))
        return [ts,]

    def parse(self, value):
        """
        Process the specified datetime.datetime value, returning a list containing
        one tuple which contains the datetime as a unix timestamp and None indicating
        there is no term metadata.

        :param value: The datetime value to parse.
        :type value: datetime.datetime
        :returns: A list of (term, metadata) tuples.
        :rtype: list
        """
        return [(self.terms(value)[0], None)]
