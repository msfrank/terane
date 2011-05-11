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

from pyparsing import alphanums, quotedString, Suppress, Word, Optional
from whoosh.query import Prefix

"""
Terane Query Language term grammar
==================================

<alphas>                ::= alphabetic characters 'a' through 'z', 'A' through 'Z'
<digits>                ::= digits 0 through 9
<unreservedSymbols>     ::= '_' | '+' | '-' | '*' | '/' | '\' | ',' | '.' | '&' | '|' | '^' | '~' | '@' | '#' | '$' | '%' | ':' | ';'
<unreservedChars>       ::= <alphas> | <digits> | <unreservedSymbols>
<quotedString>          ::= sequence of characters starting with a single or double quotaton mark, containing a span of
                            zero or more characters, and ending with a matching single or double quotation mark
<searchWord>            ::= <quotedString> | <unreservedChars>+
<searchField>           ::= <unreservedChars>+
<searchTerm>            ::= [ <searchField> '=' ] <searchWord>
"""

unreservedChars = alphanums + '_+-*/\,.&|^~@#$%:;'
fieldSeparator = Suppress('=')

searchWord = quotedString | Word(unreservedChars)
searchTerm = Optional(Word(unreservedChars) + fieldSeparator) + searchWord
def parseSearchTerm(tokens):
    if len(tokens) == 1:
        return Prefix('default', unicode(tokens[0]))
    return Prefix(tokens[0], unicode(tokens[1]))
searchTerm.setParseAction(parseSearchTerm)
