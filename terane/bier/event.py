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

import socket, datetime, copy
from dateutil.tz import tzutc
from zope.interface import implements
from terane.idgen import idgen
from terane.bier.fields import IdentityField, TextField, DatetimeField
from terane.signals import ICopyable

class Assertion(object):
    """
    """
    def __init__(self, fieldname, fieldtype, expects=False, guarantees=True, ephemeral=False, accepts=None):
        self.fieldname = fieldname
        self.fieldtype = fieldtype
        self.expects = expects
        self.guarantees = guarantees
        self.ephemeral = ephemeral
        self.accepts = accepts

class Contract(object):
    """
    """

    input = Assertion('input', IdentityField, guarantees=True, ephemeral=False)
    hostname = Assertion('hostname', IdentityField, guarantees=True, ephemeral=False)
    message = Assertion('message', TextField, guarantees=True, ephemeral=False)

    def __init__(self, *assertions):
        for assertion in assertions:
            if not isinstance(assertion, Assertion):
                raise TypeError('Contract must consist only of Assertions')
            if hasattr(self, assertion.fieldname):
                raise Exception("Assertion '%s' is already present in the Contract" % assertion.fieldname)
            object.__setattr__(self, assertion.fieldname, assertion)

    def __setattr__(self, name, value):
        raise Exception("Contract is read-only, setting attributes is not allowed")

class Event(object):
    """
    """

    implements(ICopyable)

    def __init__(self):
        self._fields = dict()
        # set ts and id
        self.ts = datetime.datetime.now(tzutc())
        try:
            self.id = idgen.allocate()
        except:
            self.id = None
        # set default values for basic fields
        self[Contract.hostname] = socket.gethostname()
        self[Contract.input] = ''
        self[Contract.message] = ''

    def __str__(self):
        return "<Event %s>" % ' '.join(["%s:%s='%s'" %(fn,ft.__name__,v) for fn,ft,v in self.fields()])

    def __contains__(self, assertion):
        if (assertion.fieldname,assertion.fieldtype) in self._fields:
            return True
        return False

    def __getitem__(self, assertion):
        return self._fields[(assertion.fieldname,assertion.fieldtype)]

    def __setitem__(self, assertion, value):
        self._fields[(assertion.fieldname,assertion.fieldtype)] = assertion.fieldtype().validate(value)

    def __delitem__(self, assertion):
        del self._fields[(assertion.fieldname,assertion.fieldtype)]

    def fields(self):
        for (name,fieldtype),value in self._fields.items():
            yield name, fieldtype, value

    def copy(self):
        return copy.deepcopy(self)
