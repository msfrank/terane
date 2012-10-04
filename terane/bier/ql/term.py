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

import pyparsing as pp
from terane.bier.matching import QueryTerm
from terane.loggers import getLogger

logger = getLogger('terane.bier.ql')


# --------------------
# keyValueTerm grammar
# --------------------
# <alphanums>             ::= alphabetic characters 'a' through 'z', 'A' through 'Z', digits 0 through 9
# <unreservedSymbols>     ::= '_' | '+' | '-' | '*' | '/' | '\' | ',' | '.' | '&' | '|' | '^' | '~' | '@' | '#' | '$' | '%' | ':' | ';'
# <unreservedChars>       ::= <alphas> | <digits> | <unreservedSymbols>
# <subjectWord>           ::= <quotedString> | <unreservedChars>+
# <subjectFieldName>      ::= <unreservedChars>+
# <subjectFieldType>      ::= <unreservedChars>+
# <keyValueTerm>          ::= <subjectWord> | <subjectFieldName> '=' <subjectWord>

fieldName = pp.Word(pp.alphanums + '_')
unquotedValue = pp.Word(pp.alphanums + '_+*/\\,.&|^~@#$%:;')
value = pp.QuotedString('\'') | pp.QuotedString('"') | unquotedValue
keyValueTerm = ( fieldName + pp.Suppress('=') + value ) | value
def parseKeyValueTerm(tokens):
    "Parse a subject term."
    if len(tokens) == 1:
        fieldname,value = None, unicode(tokens[0])
    else:
        fieldname,value = str(tokens[0]), unicode(tokens[1])
    return QueryTerm(fieldname, None, None, value)
keyValueTerm.setParseAction(parseKeyValueTerm)

# --------------------
# functionTerm grammar
# --------------------
# <typedFunctionTerm>     ::= <fieldName> '=' <fieldType> ':' <function> '(' <params> ')'
# <untypedFunctionTerm>   ::= <fieldName> '=' <function> '(' <params> ')'
# <subjectTerm>           ::= <subjectWord> | <subjectFieldName> '=' <subjectWord> | <subjectQualifiedTerm>

fieldType = pp.Word(pp.alphanums + '_')
function = pp.Word(pp.alphanums + '_')
value = pp.QuotedString('(', escChar='\\', endQuoteChar=')')
typedFunctionTerm = fieldName + pp.Suppress('=') + fieldType + pp.Suppress(':') + function + value
untypedFunctionTerm = fieldName + pp.Suppress('=') + function + value
functionTerm = typedFunctionTerm | untypedFunctionTerm
def parseFunctionTerm(tokens):
    if len(tokens) == 4:
        fieldname,fieldtype,function,value = str(tokens[0]),str(tokens[1]),str(tokens[2]),unicode(tokens[3])
    elif len(tokens) == 3:
        fieldname,fieldtype,function,value = str(tokens[0]),None,str(tokens[1]),unicode(tokens[2])
    return QueryTerm(fieldname, fieldtype, function, value)
functionTerm.setParseAction(parseFunctionTerm)

subjectTerm = functionTerm | keyValueTerm
