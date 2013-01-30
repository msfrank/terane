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
from terane.plugins import ILoadable, IPlugin, Plugin
from terane.bier.interfaces import IField
from terane.bier.matching import Term, Phrase, RangeGreaterThan, RangeLessThan
from terane.loggers import getLogger

logger = getLogger('terane.bier.schema')

class SchemaError(Exception):
    pass

class QualifiedField(object):
    """
    """
    def __init__(self, fieldname, fieldtype, field):
        self.fieldname = unicode(fieldname)
        self.fieldtype = unicode(fieldtype)
        self.field = field

    def parseValue(self, value):
        return self.field.parseValue(value)

    def makeMatcher(self, fieldfunc, value):
        return self.field.makeMatcher(self, fieldfunc, value)

    def __str__(self):
        return "%s:%s" % (self.fieldtype, self.fieldname)
        
class BaseField(object):
    """
    """
    def __init__(self, plugin):
        pass

    def makeMatcher(self, field, fieldfunc, value):
        if fieldfunc:
            try:
                func = getattr(self, "match_%s" % fieldfunc)
            except:
                raise NotImplementedError("'%s' field method not implemented" % fieldfunc)
        else:
            func = self.defaultMatcher
        return func(field, value)

    def defaultMatcher(self, field, value):
        raise NotImplementedError("Default field method not implemented")

class IdentityField(BaseField):
    """
    IdentityField stores data as-is, without any linguistic processing.
    """

    implements(IField)

    def validateValue(self, value):
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

    def parseValue(self, value):
        """
        Return a list of tuples, each containing a tokenized term and a dict
        containing term metadata.  The metadata for an IdentityField term is the
        'pos' item which is a list of term positions.

        :param value: The value to parse.
        :type value: str or unicode
        :returns: A list of (term, metadata) tuples.
        :rtype: list
        """
        return [(unicode(value), {u'pos': 0})]

    def match_is(self, field, value):
        """
        The IdentityField tokenizer just converts the value to unicode,
        if necessary.

        :param value: The value to tokenize.
        :type value: str or unicode
        :returns: The value as a unicode object.
        :rtype: unicode
        """
        return Term(field, self.validateValue(value))

    defaultMatcher = match_is

class TextField(BaseField):
    """
    TextField stores input by breaking it into tokens separated by whitespace or
    any character other than an underscore, numeral, or alphanumeric character
    (as designated by the unicode properties database).  Tokens in a TextField are
    always lowercased, so queries on a TextField are case-insensitive.
    """

    implements(IField)

    def validateValue(self, value):
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

    def parseValue(self, value):
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
        terms = [t.lower() for t in re.split(r'\W+', value, re.UNICODE) if t != '']
        positions = {}
        for position in range(len(terms)):
            term = terms[position]
            if term in positions:
                positions[term][u'pos'].append(position)
            else:
                positions[term] = {u'pos': [position]}
        return positions.items()

    def match_in(self, field, value):
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
        value = self.validateValue(value)
        terms = [t.lower() for t in re.split(r'\W+', value, re.UNICODE) if t != '']
        if len(terms) == 0:
            return None
        if len(terms) == 1:
            return Term(field, terms[0])
        return Phrase(field, terms)

    defaultMatcher = match_in

class IntegerField(BaseField):
    """
    IntegerField stores a python int or long.
    """

    implements(IField)

    def validateValue(self, value):
        """
        Validate that the supplied value is an int or long and is not larger
        than 64-bit signed/unsigned.

        :param value: The value to validate.
        :type value: int or long
        :returns: The value
        :rtype: int or long
        :raises TypeError: The value is of the wrong type.
        """
        if not isinstance(value, int) and not isinstance(value, long):
            raise TypeError('value must be of type int or long')
        if value < -(2**63) or value > ((2**64) - 1):
            raise OverflowError("value %i is out of range" % value)
        return value

    def parseValue(self, value):
        """
        Process the specified integer value, returning a list containing one
        tuple which contains the integer and None indicating there is no term
        metadata.

        :param value: The integer value to parse.
        :type value: int or long
        :returns: A list of (term, metadata) tuples.
        :rtype: list
        """
        return [(value, None)]

    def _str2int(self, value):
        value = value.strip().lower()
        if value.startswith('0x'):
            value = int(value, 16)
        elif value.startswith('0'):
            value = int(value, 8)
        else:
            value = int(value, 10)
        return self.validateValue(value)

    def match_is(self, field, value):
        """
        Process the specified integer value, converting it to a unix
        timestamp.

        :param value: The string value to tokenize.
        :type value: unicode or str
        :returns: A list of tokenized terms.
        :rtype: list
        """
        return Term(field, self._str2int(value))

    defaultMatcher = match_is

    def match_gt(self, field, value):
        return RangeGreaterThan(field, self._str2int(value), exclusive=True)

    def match_lt(self, field, value):
        return RangeLessThan(field, self._str2int(value), exclusive=True)

    def match_ge(self, field, value):
        return RangeGreaterThan(field, self._str2int(value), exclusive=False)

    def match_le(self, field, value):
        return RangeLessThan(field, self._str2int(value), exclusive=False)

class DatetimeField(BaseField):
    """
    DatetimeField stores a python datetime.datetime.
    """

    implements(IField)

    def validateValue(self, value):
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

    def parseValue(self, value):
        """
        Process the specified datetime.datetime value, returning a list containing
        one tuple which contains the datetime as a unix timestamp and None indicating
        there is no term metadata.

        :param value: The datetime value to parse.
        :type value: datetime.datetime
        :returns: A list of (term, metadata) tuples.
        :rtype: list
        """
        return [(int(calendar.timegm(value.timetuple())), None)]

    def match_is(self, field, value):
        """
        Process the specified datetime.datetime value, converting it to a unix
        timestamp.

        :param value: The string value to tokenize.
        :type value: unicode or str
        :returns: A list of tokenized terms.
        :rtype: list
        """
        value = datetime.datetime.strptime(value, "%Y/%m/%dT%H:%M:%S")
        value = self.validateValue(value)
        ts = int(calendar.timegm(value.timetuple()))
        return Term(field, ts)

class BaseFieldPlugin(Plugin):
    implements(IPlugin)
    components = [
        (IdentityField, IField, 'literal'),
        (TextField, IField, 'text'),
        (IntegerField, IField, 'int'),
        (DatetimeField, IField, 'datetime'),
        ]
