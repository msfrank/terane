import os, sys
from twisted.trial import unittest
from terane.bier.ql import parseIterQuery, parseTailQuery

class iter_Tests(unittest.TestCase):
    """iter tests."""

    def test_one_term(self):
        q = parseIterQuery("foo")

    def test_single_quoted_term(self):
        q = parseIterQuery("'foo'")

    def test_double_quoted_term(self):
        q = parseIterQuery('"foo"')

    def test_AND_operator(self):
        q = parseIterQuery('foo AND bar')

    def test_OR_operator(self):
        q = parseIterQuery('foo OR bar')

class DATE_Tests(unittest.TestCase):
    """DATE clause tests."""

    def test_DATE_FROM(self):
        q = parseIterQuery("WHERE DATE FROM 2000/1/1T12:00:00")

    def test_DATE_TO(self):
        q = parseIterQuery("WHERE DATE TO 2000/1/1T12:00:00")

    def test_DATE_FROM_TO_clause(self):
        q = parseIterQuery("WHERE DATE FROM 2000/1/1 TO 2010/1/1")

    def test_DATE_TO_FROM_clause(self):
        q = parseIterQuery("WHERE DATE TO 2000/1/1 FROM 2010/1/1")

    def test_DATE_absolute_date_and_time(self):
        q = parseIterQuery("WHERE DATE FROM 2000/1/1T12:00:00")

    def test_DATE_relative_date_weeks(self):
        q = parseIterQuery("WHERE DATE FROM 1 WEEK AGO")
        q = parseIterQuery("WHERE DATE FROM 1 WEEKS AGO")

    def test_DATE_relative_date_days(self):
        q = parseIterQuery("WHERE DATE FROM 1 DAY AGO")
        q = parseIterQuery("WHERE DATE FROM 1 DAYS AGO")

    def test_DATE_relative_date_hours(self):
        q = parseIterQuery("WHERE DATE FROM 1 HOUR AGO")
        q = parseIterQuery("WHERE DATE FROM 1 HOURS AGO")

    def test_DATE_relative_date_minutes(self):
        q = parseIterQuery("WHERE DATE FROM 1 MINUTE AGO")
        q = parseIterQuery("WHERE DATE FROM 1 MINUTES AGO")

    def test_DATE_relative_date_seconds(self):
        q = parseIterQuery("WHERE DATE FROM 1 SECOND AGO")
        q = parseIterQuery("WHERE DATE FROM 1 SECONDS AGO")
