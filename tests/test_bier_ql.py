import os, sys
from zope.interface import implements
from twisted.trial import unittest
from terane.bier.ql import parseIterQuery, parseTailQuery
from terane.bier.interfaces import IIndex, ISchema
from terane.bier.fields import QualifiedField, TextField, IdentityField, IntegerField
from terane.bier.matching import QueryTerm, Term

class MockIndex(object):
    implements(IIndex, ISchema)
    def __init__(self):
        self._fields = {
            (None, None): QualifiedField('message', 'text', TextField(None)),
            ('message', 'text'): QualifiedField('message', 'text', TextField(None)),
            ('message', None): QualifiedField('message', 'text', TextField(None)),
            ('input', 'literal'): QualifiedField('input', 'literal', IdentityField(None)),
            ('input', None): QualifiedField('input', 'literal', IdentityField(None)),
            ('number', 'int'): QualifiedField('number', 'int', IntegerField(None)),
            ('number', None): QualifiedField('number', 'int', IntegerField(None)),
        }
    def getSchema(self):
        return self
    def newSearcher(self):
        raise NotImplementedError()
    def newWriter(self):
        raise NotImplementedError()
    def getStats(self):
        raise NotImplementedError()
    def hasField(self, fieldname, fieldtype):
        return True if (fieldname,fieldtype) in self._fields else False
    def getField(self, fieldname, fieldtype):
        return self._fields[(fieldname,fieldtype)]
    def addField(self, fieldname, fieldtype):
        raise NotImplementedError()

class iter_Tests(unittest.TestCase):
    """iter tests."""
    def setUp(self):
        self.index = MockIndex()

    def test_one_term(self):
        q,p = parseIterQuery("foo")
        self.assertEqual(q, QueryTerm(None, None, None, "foo"))
        o = q.optimizeMatcher(self.index)
        self.assertEqual(o, Term(self._fields[(None, None)], "foo"))

    def test_single_quoted_term(self):
        q,p = parseIterQuery("'foo'")

    def test_double_quoted_term(self):
        q,p = parseIterQuery('"foo"')

    def test_AND_operator(self):
        q,p = parseIterQuery('foo AND bar')

    def test_OR_operator(self):
        q,p = parseIterQuery('foo OR bar')

    def test_NOT_operator(self):
        q,p = parseIterQuery('NOT bar')

    def test_phrase_term(self):
        q,p = parseIterQuery('"foo bar baz"')

    def test_term_function(self):
        q,p = parseIterQuery('message=text:is(foo)')

class DATE_Tests(unittest.TestCase):
    """DATE clause tests."""

    def test_DATE_FROM(self):
        q,p = parseIterQuery("ALL WHERE DATE FROM 2000/1/1T12:00:00")

    def test_DATE_TO(self):
        q,p = parseIterQuery("ALL WHERE DATE TO 2000/1/1T12:00:00")

    def test_DATE_FROM_TO_clause(self):
        q,p = parseIterQuery("ALL WHERE DATE FROM 2000/1/1 TO 2010/1/1")

    def test_DATE_TO_FROM_clause(self):
        q,p = parseIterQuery("ALL WHERE DATE TO 2000/1/1 FROM 2010/1/1")

    def test_DATE_absolute_date_and_time(self):
        q,p = parseIterQuery("ALL WHERE DATE FROM 2000/1/1T12:00:00")

    def test_DATE_relative_date_weeks(self):
        q,p = parseIterQuery("ALL WHERE DATE FROM 1 WEEK AGO")
        q,p = parseIterQuery("ALL WHERE DATE FROM 1 WEEKS AGO")

    def test_DATE_relative_date_days(self):
        q,p = parseIterQuery("ALL WHERE DATE FROM 1 DAY AGO")
        q,p = parseIterQuery("ALL WHERE DATE FROM 1 DAYS AGO")

    def test_DATE_relative_date_hours(self):
        q,p = parseIterQuery("ALL WHERE DATE FROM 1 HOUR AGO")
        q,p = parseIterQuery("ALL WHERE DATE FROM 1 HOURS AGO")

    def test_DATE_relative_date_minutes(self):
        q,p = parseIterQuery("ALL WHERE DATE FROM 1 MINUTE AGO")
        q,p = parseIterQuery("ALL WHERE DATE FROM 1 MINUTES AGO")

    def test_DATE_relative_date_seconds(self):
        q,p = parseIterQuery("ALL WHERE DATE FROM 1 SECOND AGO")
        q,p = parseIterQuery("ALL WHERE DATE FROM 1 SECONDS AGO")
