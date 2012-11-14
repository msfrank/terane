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

import re, time, dateutil.parser
from zope.interface import implements
from terane.plugins import Plugin, IPlugin
from terane.filters import Filter, IFilter, FilterError
from terane.bier.event import Contract, Assertion
from terane.loggers import getLogger

logger = getLogger("terane.filters.apache")

class ApacheCommonFilter(Filter):

    implements(IFilter)

    def configure(self, section):
        self._regex = re.compile(r'''
                (?P<remotehost>[\d.]+)\ 
                (?P<remotelog>\S+)\ 
                (?P<remoteuser>\S+)\ 
                \[(?P<date>[\w:/]+\s[+\-]\d{4})\]\ 
                \"(?P<request>.+?)\"\ 
                (?P<status>\d{3})\ 
                (?P<byteswritten>\d+)''',
            re.VERBOSE
            )
        self._contract = Contract()
        self._contract.addAssertion(u'remotehost', u'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion(u'remotelog', u'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion(u'remoteuser', u'literal', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion(u'request', u'literal', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion(u'status', u'int', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion(u'byteswritten', u'int', expects=False, guarantees=True, ephemeral=False)
        self._contract.sign()

    def getContract(self):
        return self._contract

    def filter(self, event):
        line = event[self._contract.field_message]
        m = self._regex.match(line)
        if m == None:
            raise FilterError("incoming line '%s' didn't match regex" % line)
        date = m.group('date')
        if date == None:
            raise FilterError("regex did not match 'date'")
        # parse the timestamp
        try:
            event.ts = dateutil.parser.parse(date, dayfirst=True, fuzzy=True)
        except Exception, e:
            raise FilterError("failed to parse date '%s': %s" % (date, e))
        # extract each field
        for assertion in self._contract:
            if assertion.fieldname in ('message', 'hostname', 'input'):
                continue
            value = m.group(assertion.fieldname)
            if value == None:
                raise FilterError("regex did not match '%s'" % assertion.fieldname)
            if assertion.fieldtype == u'int':
                value = int(value)
            event[assertion] = value
        return event

class ApacheCombinedFilter(Filter):

    implements(IFilter)

    def configure(self, section):
        self._regex = re.compile(r'''
                (?P<remotehost>[\d.]+)\ 
                (?P<remotelog>\S+)\ 
                (?P<remoteuser>\S+)\ 
                \[(?P<date>[\w:/]+\s[+\-]\d{4})\]\ 
                \"(?P<request>.+?)\"\ 
                (?P<status>\d{3})\ 
                (?P<byteswritten>\d+)\ 
                \"(?P<referrer>[^\"]+)\"\ 
                \"(?P<useragent>[^\"]+)\"''',
            re.VERBOSE
            )
        self._contract = Contract()
        self._contract.addAssertion(u'remotehost', u'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion(u'remotelog', u'literal', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion(u'remoteuser', u'literal', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion(u'request', u'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion(u'status', u'int', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion(u'byteswritten', u'int', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion(u'referrer', u'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.addAssertion(u'useragent', u'text', expects=False, guarantees=True, ephemeral=False)
        self._contract.sign()

    def getContract(self):
        return self._contract

    def filter(self, event):
        line = event[self._contract.message]
        m = self._regex.match(line)
        if m == None:
            raise FilterError("incoming line '%s' didn't match regex" % line)
        date = m.group('date')
        if date == None:
            raise FilterError("regex did not match 'date'")
        # parse the timestamp
        try:
            event.ts = dateutil.parser.parse(date, dayfirst=True, fuzzy=True)
        except Exception, e:
            raise FilterError("failed to parse date '%s': %s" % (date, e))
        # extract each field
        for assertion in self._contract:
            if assertion.fieldname in ('message', 'hostname', 'input'):
                continue
            value = m.group(assertion.fieldname)
            if value == None:
                raise FilterError("regex did not match '%s'" % assertion.fieldname)
            if assertion.fieldtype == u'int':
                value = int(value)
            event[assertion] = value
        return event

class ApacheFilterPlugin(Plugin):
    implements(IPlugin)
    components = [
        (ApacheCombinedFilter, IFilter, 'apache_combined'),
        (ApacheCommonFilter, IFilter, 'apache_common'),
        ]
