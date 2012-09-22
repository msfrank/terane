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

import socket, datetime, copy, re
from dateutil.tz import tzutc
from zope.interface import implements
from terane.idgen import idgen
from terane.bier import IField
from terane.bier.fields import IdentityField, TextField, DatetimeField
from terane.signals import ICopyable

class Assertion(object):
    """
    """
    def __init__(self, fieldname, fieldtype, expects=False, guarantees=True, ephemeral=False, accepts=None):
        if len(fieldname) < 2:
            raise TypeError("fieldname must be at least two characters")
        if fieldname.startswith('__'):
            raise TypeError("fieldname must not start with two underscores")
        if not re.match(r'[a-zA-Z0-9_]+', fieldname):
            raise TypeError("fieldname must consist of only alphanumeric characters or underscores")
        self.fieldname = fieldname
        if not IField.implementedBy(fieldtype):
            raise TypeError("fieldtype must implement IField")
        self.fieldtype = fieldtype
        self.expects = bool(expects)
        self.guarantees = bool(guarantees)
        self.ephemeral = bool(ephemeral)
        self.accepts = accepts

class Contract(object):
    """
    """

    def __init__(self):
        self.signed = False
        self._assertions = {}
        self.addAssertion('input', IdentityField, guarantees=True, ephemeral=False)
        self.addAssertion('hostname', IdentityField, guarantees=True, ephemeral=False)
        self.addAssertion('message', TextField, guarantees=True, ephemeral=False)

    def addAssertion(self, fieldname, fieldtype, **kwds):
        if self.signed:
            raise Exception("writing to a signed Contract is not allowed")
        assertion = Assertion(fieldname, fieldtype, **kwds)
        if fieldname in self._assertions:
            raise Exception("'%s' is already present in the Contract" % fieldname)
        self._assertions[fieldname] = assertion
        return self

    def fields(self):
        return self._assertions.iteritems()

    def sign(self):
        if self.signed == True:
            raise ValueError("Contract is already signed")
        self.signed = True
        self.__setattr__ = self._setReadonly
        return self

    def validateContract(self, prior):
        """
        Validate the contract against the specified prior contract.

        :param prior: The contract to validate against.
        :type prior: :class:`terane.bier.event.Contract`
        :returns: A new signed contract which is the union of the current and prior
          contracts.
        :rtype: :class:`terane.bier.event.Contract`
        :raises ValueError: The current or prior contract is unsigned. 
        :raises Exception: The validation failed. 
        """
        if not self.signed or not prior.signed:
            raise ValueError("validating against an unsigned Contract is not allowed")
        guaranteed = set([(a.fieldname,a.fieldtype)
                          for a in prior._assertions.values()
                          if a.guarantees == True])
        expected = set([(a.fieldname,a.fieldtype)
                          for a in self._assertions.values()
                          if a.expects == True])
        if not expected.issubset(guaranteed):
            raise Exception("filtering chain schemas are not compatible")
        contract = Contract()
        contract._assertions.update(prior._assertions)
        contract._assertions.update(self._assertions)
        return contract.sign()

    def validateEvent(self, event):
        """
        Validate the specified event against the contract.

        :param event: The event to validate.
        :type event: :class:`terane.bier.event.Event`
        :returns: The validated event.
        :type event: :class:`terane.bier.event.Event`
        :raises Exception: The validation failed.
        """
        return event

    def _setReadonly(self, name, value):
        raise Exception("writing to a signed Contract is not allowed")

    def __getattr__(self, name):
        if name.startswith('field_'):
            f,sep,fieldname = name.partition('_')
            return self._assertions[fieldname]

    def __setattr__(self, name, value):
        if name.startswith('field_'):
            raise Exception("can't set attribute '%s' in the field namespace" % name)
        object.__setattr__(self, name, value)

    def __len__(self):
        return len(self._assertions)

    def __iter__(self):
        return self._assertions.itervalues()

class Event(object):
    """
    """

    implements(ICopyable)

    _contract = Contract()

    def __init__(self):
        self._fields = dict()
        # set ts and id
        self.ts = datetime.datetime.now(tzutc())
        try:
            self.id = idgen.allocate()
        except:
            self.id = None
        # set default values for basic fields
        self[Event._contract.field_hostname] = socket.gethostname()
        self[Event._contract.field_input] = ''
        self[Event._contract.field_message] = ''

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
