import os, sys
from twisted.trial import unittest
from terane.loggers import StdoutHandler, startLogging, TRACE
from terane.bier.fields import IdentityField, TextField
from terane.bier.event import Assertion, Contract, Event

class Contract_Tests(unittest.TestCase):
    """Contract tests."""

    def test_create_contract(self):
        contract = Contract()
        # validate initial fields
        self.failUnless(hasattr(contract, 'field_input') == True)
        self.failUnless(contract.field_input.fieldname == 'input')
        self.failUnless(contract.field_input.fieldtype == IdentityField)
        self.failUnless(contract.field_input.guarantees == True)
        self.failUnless(contract.field_input.ephemeral == False)
        self.failUnless(hasattr(contract, 'field_hostname') == True)
        self.failUnless(contract.field_hostname.fieldname == 'hostname')
        self.failUnless(contract.field_hostname.fieldtype == IdentityField)
        self.failUnless(contract.field_hostname.guarantees == True)
        self.failUnless(contract.field_hostname.ephemeral == False)
        self.failUnless(hasattr(contract, 'field_message') == True)
        self.failUnless(contract.field_message.fieldname == 'message')
        self.failUnless(contract.field_message.fieldtype == TextField)
        self.failUnless(contract.field_message.guarantees == True)
        self.failUnless(contract.field_message.ephemeral == False)
        # add a custom field
        contract.addAssertion('test', IdentityField, guarantees=True, ephemeral=False)
        self.failUnless(hasattr(contract, 'field_test') == True)
        self.failUnless(contract.field_test.fieldname == 'test')
        self.failUnless(contract.field_test.fieldtype == IdentityField)
        self.failUnless(contract.field_test.guarantees == True)
        self.failUnless(contract.field_test.ephemeral == False)
        
    def test_check_contract(self):
        contract = Contract()
        contract.addAssertion('test', IdentityField, guarantees=True, ephemeral=False)
        # test len method
        self.failUnless(len(contract) == 4)
        # test iter method
        for asrt in contract:
            self.failUnless(isinstance(asrt, Assertion))
            self.failUnlessIn(asrt.fieldname, ('input','hostname','message','test'))
        # test field iteration method
        for fieldname,fieldtype in contract.fields():
            self.failUnlessIn(fieldname, ('input','hostname','message','test'))

    def test_sign_contract(self):
        contract = Contract()
        contract.addAssertion('test', IdentityField, guarantees=True, ephemeral=False)
        # sign the contract, no more modifications allowed
        contract.sign()
        self.failUnlessRaises(Exception, contract.addAssertion, 'fails', TextField, guarantees=True, ephemeral=False)

    def test_validate_contract_succeeds(self):
        contract = Contract().sign()
        prior = Contract().sign()
        contract.validateContract(prior)

    def test_validate_contract_expects_missing_field(self):
        prior = Contract().sign()
        contract = Contract().addAssertion('test', TextField, expects=True).sign()
        self.failUnlessRaises(Exception, contract.validatesAgainst, prior)
