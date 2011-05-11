import os, sys
from twisted.trial import unittest
from terane.dql import Query

class DATE_Clause_Tests(unittest.TestCase):
    """DQL DATE clause tests."""

    def test_DATE_FROM(self):
        q = Query("DATE FROM 2000/1/1T12:00:00")

    def test_DATE_TO(self):
        q = Query("DATE TO 2000/1/1T12:00:00")

    def test_DATE_FROM_TO_clause(self):
        q = Query("DATE FROM 2000/1/1 TO 2010/1/1")

    def test_DATE_TO_FROM_clause(self):
        q = Query("DATE TO 2000/1/1 FROM 2010/1/1")

    def test_DATE_absolute_date_and_time(self):
        q = Query("DATE FROM 2000/1/1T12:00:00")

    def test_DATE_relative_date_weeks(self):
        q = Query("DATE FROM 1 WEEK AGO")
        q = Query("DATE FROM 1 WEEKS AGO")

    def test_DATE_relative_date_days(self):
        q = Query("DATE FROM 1 DAY AGO")
        q = Query("DATE FROM 1 DAYS AGO")

    def test_DATE_relative_date_hours(self):
        q = Query("DATE FROM 1 HOUR AGO")
        q = Query("DATE FROM 1 HOURS AGO")

    def test_DATE_relative_date_minutes(self):
        q = Query("DATE FROM 1 MINUTE AGO")
        q = Query("DATE FROM 1 MINUTES AGO")

    def test_DATE_relative_date_seconds(self):
        q = Query("DATE FROM 1 SECOND AGO")
        q = Query("DATE FROM 1 SECONDS AGO")

class ID_Clause_Tests(unittest.TestCase):
    """DQL ID clause tests. """

    def test_ID_FROM(self):
        q = Query("ID FROM 1")

    def test_ID_TO(self):
        q = Query("ID TO 2")

    def test_ID_FROM_TO_clause(self):
        q = Query("ID FROM 1 TO 2")

    def test_ID_TO_FROM_clause(self):
        q = Query("ID TO 2 FROM 1")
