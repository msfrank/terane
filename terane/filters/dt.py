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

import re, time, email.utils, datetime
from zope.interface import implements
from terane.plugins import Plugin, IPlugin
from terane.filters import Filter, IFilter, FilterError, StopFiltering
from terane.bier.event import Contract, Assertion
from terane.loggers import getLogger

logger = getLogger("terane.filters.datetime")

class RFC2822DatetimeFilter(Filter):
    """
    Given an event field with a RFC2822-compatible datetime string, convert the
    string to a datetime.datetime and store it as the event ts.
    """

    implements(IFilter)

    def configure(self, section):
        self._fieldname = section.getString('source field', 'rfc2822_date')
        self._expected = section.getBoolean('expects source', False)
        self._guaranteed = section.getBoolean('guarantees source', False)
        self._contract = Contract()
        self._contract.addAssertion( self._fieldname, 'text',
            expects=self._expected, guarantees=self._guaranteed, ephemeral=True)
        self._assertion = getattr(self._contract, 'field_' + self._fieldname)
        self._contract.sign()

    def getContract(self):
        return self._contract

    def filter(self, event):
        try:
            ts = email.utils.parsedate_tz(event[self._assertion])
            event.ts = datetime.datetime.fromtimestamp(time.mktime(ts))
            return event
        except Exception, e:
            raise FilterError("failed to update ts: %s" %  e)

class SyslogDatetimeFilter(Filter):
    """
    Given an event field with a syslog-compatible datetime string, convert the
    string to a datetime.datetime and store as the event ts.
    """

    implements(IFilter)

    def configure(self, section):
        self._fieldname = section.getString('source field', 'syslog_date')
        self._expected = section.getBoolean('expects source', False)
        self._guaranteed = section.getBoolean('guarantees source', False)
        self._contract = Contract()
        self._contract.addAssertion(self._fieldname, 'text',
            expects=self._expected, guarantees=self._guaranteed, ephemeral=True)
        self._assertion = getattr(self._contract, 'field_' + self._fieldname)
        self._contract.sign()

    def getContract(self):
        return self._contract

    def filter(self, event):
        try:
            date = event[self._assertion]
            # if there is no leading zero in front of the day, then add it
            if date[4] == ' ':
                date = list(date)
                date[4] = '0'
                date = ''.join(date)
            # append the year to the date string
            date += ' %i' % time.localtime()[0]
            # parse the date string into a struct_time
            ts = time.strptime(date, "%b %d %H:%M:%S %Y")
            event.ts = datetime.datetime.fromtimestamp(time.mktime(ts))
            return event
        except Exception, e:
            raise FilterError("failed to update ts: %s" %  e)

class DatetimeExpanderFilter(Filter):
    """
    Expand the event ts into separate fields for each datetime component.
    """

    implements(IFilter)

    def configure(self, section):
        self._contract = Contract()
        self._contract.addAssertion('dt_year', 'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion('dt_month', 'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion('dt_day', 'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion('dt_hour', 'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion('dt_minute', 'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion('dt_second', 'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion('dt_weekday', 'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion('dt_yearday', 'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.sign()

    def getContract(self):
        return self._contract

    def filter(self, event):
        try:
            tm = event.ts.timetuple()
            event[self._contract.field_dt_year] = tm.tm_year
            event[self._contract.field_dt_month] = tm.tm_mon
            event[self._contract.field_dt_day] = tm.tm_mday
            event[self._contract.field_dt_hour] = tm.tm_hour
            event[self._contract.field_dt_minute] = tm.tm_min
            event[self._contract.field_dt_second] = tm.tm_sec
            event[self._contract.field_dt_weekday] = tm.tm_wday
            event[self._contract.field_dt_yearday] = tm.tm_yday
            return event
        except Exception, e:
            raise FilterError("failed to expand ts: %s" % e)

class DatetimeFilterPlugin(Plugin):
    implements(IPlugin)
    components = [
        (RFC2822DatetimeFilter, IFilter, 'dt_rfc2822'),
        (SyslogDatetimeFilter, IFilter, 'dt_syslog'),
        (DatetimeExpanderFilter, IFilter, 'dt_expander'),
        ]
