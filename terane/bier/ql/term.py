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
from terane.bier.matching import Term, Phrase
from terane.loggers import getLogger

logger = getLogger('terane.bier.ql')

# -------------------
# subjectTerm grammar
# -------------------
# <alphanums>             ::= alphabetic characters 'a' through 'z', 'A' through 'Z', digits 0 through 9
# <unreservedSymbols>     ::= '_' | '+' | '-' | '*' | '/' | '\' | ',' | '.' | '&' | '|' | '^' | '~' | '@' | '#' | '$' | '%' | ':' | ';'
# <unreservedChars>       ::= <alphas> | <digits> | <unreservedSymbols>
# <quotedString>          ::= sequence of characters starting with a single or double quotaton mark, containing a span of
#                             zero or more characters, and ending with a matching single or double quotation mark
# <subjectWord>           ::= <quotedString> | <unreservedChars>+
# <subjectFieldName>      ::= <unreservedChars>+
# <subjectFieldType>      ::= <unreservedChars>+
# <subjectFieldOperator>  ::= <unreservedChars>+
# <subjectQualifiedTerm>  ::= <subjectFieldName> ':' [ <subjectFieldType> ':' ] <subjectFieldOperator> '(' <subjectWord> ')'
# <subjectTerm>           ::= <subjectWord> | <subjectFieldName> '=' <subjectWord> | <subjectQualifiedTerm>

unreservedChars = pp.alphanums + '_+-*/\,.&|^~@#$%:;'
fieldSeparator = pp.Suppress('=')
subjectWord = pp.quotedString | pp.Word(unreservedChars)
subjectTerm = ( pp.Word(unreservedChars) + fieldSeparator + subjectWord ) | subjectWord
def parseSubjectTerm(tokens):
    "Parse a subject term."
    if len(tokens) == 1:
        fieldname,term = None, unicode(tokens[0])
    else:
        fieldname,term = str(tokens[0]), unicode(tokens[1])
    if term[0] == '\"' and term[-1] == '\"':
        return Phrase(fieldname, term[1:-1])
    if term[0] == '\'' and term[-1] == '\'':
        return Term(fieldname, term[1:-1])
    return Term(fieldname, term)
subjectTerm.setParseAction(parseSubjectTerm)
