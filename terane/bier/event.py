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

import datetime, copy, re
from collections import MutableMapping
from dateutil.tz import tzutc
from zope.interface import implements
from terane.signals import ICopyable

class Assertion(object):
    """
    An assertion describes a field and its invariants, pre-conditions and
    post-conditions with respect to the event processing pipeline.
    """
    def __init__(self, fieldname, fieldtype, expects=False, guarantees=True, ephemeral=False, accepts=None):
        if not isinstance(fieldname, unicode):
            raise TypeError("fieldname must be a unicode object")
        if not isinstance(fieldtype, unicode):
            raise TypeError("fieldtype must be a unicode object")
        if len(fieldname) < 2:
            raise TypeError("fieldname must be at least two characters")
        if fieldname.startswith('__'):
            raise TypeError("fieldname must not start with two underscores")
        if not fieldname[0] == '_' and not fieldname[0].isalpha():
            raise TypeError("fieldname must start with an alphabetic character or underscore")
        if not re.match(r'[a-zA-Z0-9_]+', fieldname):
            raise TypeError("fieldname must consist of only alphanumeric characters or underscores")
        self.fieldname = fieldname
        self.fieldtype = fieldtype
        self.expects = bool(expects)
        self.guarantees = bool(guarantees)
        self.ephemeral = bool(ephemeral)
        self.accepts = accepts

_assertion_input = Assertion(u'input', u'literal', guarantees=True, ephemeral=False)
_assertion_hostname = Assertion(u'hostname', u'literal', guarantees=True, ephemeral=False)
_assertion_message = Assertion(u'message', u'text', guarantees=True, ephemeral=False)

class Contract(object):
    """
    A Contract is comprised of Assertions and describes an events invariants,
    pre-conditions and post-conditions with respect to the event processing
    pipeline.  A Contract has a state of signed or unsigned; when it is unsigned,
    its Assertions may be added, removed, or modified.  Once the Contract has
    been signed, it is no longer mutable, but only signed Contracts may be used
    to validate events.
    """

    def __init__(self):
        self.signed = False
        self._assertions = {
            u'message': _assertion_message,
            u'input': _assertion_input,
            u'hostname': _assertion_hostname
            }

    def addAssertion(self, fieldname, fieldtype, **kwds):
        """
        Add an assertion to the contract.

        :param fieldname: The name of the field.
        :type fieldname: unicode
        :param fieldtype: The type of the field.
        :type fieldtype: unicode
        :param kwds: keyword parameters which further describe the assertion.
        :type kwds: dict
        :returns: A reference to the contract, so methods can be chained.
        :rtype: :class:`terane.bier.event.Contract`
        """
        if not isinstance(fieldname, unicode):
            raise TypeError("fieldname must be a unicode object")
        if not isinstance(fieldtype, unicode):
            raise TypeError("fieldtype must be a unicode object")
        if self.signed:
            raise Exception("writing to a signed Contract is not allowed")
        if fieldname in self._assertions:
            raise Exception("Assertion is already present in the Contract" % fieldname)
        assertion = Assertion(fieldname, fieldtype, **kwds)
        self._assertions[fieldname] = assertion
        return self

    def sign(self):
        """
        Sign the contract, which will prevent any further modification.

        :returns: A reference to the contract, so methods can be chained.
        :rtype: :class:`terane.bier.event.Contract`
        """
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

    def validateEventBefore(self, event, fieldstore):
        """
        Validate the specified event against the contract before it is processed.

        :param event: The event to validate.
        :type event: :class:`terane.bier.event.Event`
        :raises Exception: The validation failed.
        """
        for assertion in self._assertions.itervalues():
            if assertion.expects and not assertion in event:
                raise Exception("expected field %s is not present" % assertion.fieldname)

    def validateEventAfter(self, event, fieldstore):
        """
        Validate the specified event against the contract after it is processed.

        :param event: The event to validate.
        :type event: :class:`terane.bier.event.Event`
        :raises Exception: The validation failed.
        """
        for assertion in self._assertions.itervalues():
            hasvalue = assertion in event
            if not assertion.guarantees and not hasvalue:
                continue
            if assertion.guarantees and not hasvalue:
                raise Exception("guaranteed field %s is not present" % assertion.fieldname)
            field = fieldstore.getField(assertion.fieldtype)
            event[assertion] = field.validateValue(event[assertion])

    def finalizeEvent(self, event):
        """
        Remove all ephemeral fields from the event.

        :param event: The event to finalize.
        :type event: :class:`terane.bier.event.Event`
        :returns: The finalized event.
        :rtype event: :class:`terane.bier.event.Event`
        :raises Exception: The validation failed.
        """
        for assertion in self:
            if assertion.ephemeral and assertion in event:
                del event[assertion]    
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

class Event(MutableMapping):
    """
    """

    implements(ICopyable)

    def __init__(self, ts, offset):
        self._values = dict()
        self.ts = ts
        self.offset = offset

    def copy(self):
        return copy.deepcopy(self)

    def __str__(self):
        fields = ' '.join(["%s=%s(%s)" %(n,t,v) for n,t,v in self])
        return "<Event ts=%i offset=%i %s>" % (self.ts, self.offset, fields)

    def __len__(self):
        return len(self._values)

    def __iter__(self):
        for fieldname,(fieldtype,fieldvalue) in self._values.iteritems():
            yield fieldname,fieldtype,fieldvalue

    def __contains__(self, assertion):
        if not isinstance(assertion, Assertion):
            raise TypeError("parameter must be of type Assertion")
        if assertion.fieldname in self._values:
            return True
        return False

    def __getitem__(self, assertion):
        if not isinstance(assertion, Assertion):
            raise TypeError("parameter must be of type Assertion")
        fieldtype,value = self._values[assertion.fieldname]
        return value

    def __setitem__(self, assertion, value):
        if not isinstance(assertion, Assertion):
            raise TypeError("parameter must be of type Assertion")
        self._values[assertion.fieldname] = (assertion.fieldtype, value)

    def __delitem__(self, assertion):
        if not isinstance(assertion, Assertion):
            raise TypeError("parameter must be of type Assertion")
        del self._values[assertion.fieldname]
