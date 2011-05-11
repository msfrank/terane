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

from pyparsing import oneOf, srange, nums, Word

"""
Terane Query Language restriction grammar
=========================================

<restrictionOperator>   ::= '<' | '<=' | '>' | '>=' | '==' | !='
<digitsExceptZero>      ::= digits 1 through 9
<naturalNumber>         ::= '0' | <numberExceptZero <number>*
<restriction>           ::= <restrictionOperator> <naturalNumber>
"""

class Restriction(object):
    def __init__(self, compare=lambda x,y: True, quantity=0):
        self.compare = compare
        self.quantity = quantity
    def isSatisfied(self, results):
        compare = self.compare
        return compare(len(results), int(self.quantity))

lt = Word('<').setParseAction(lambda tokens: lambda x,y: x < y)
gt = Word('>').setParseAction(lambda tokens: lambda x,y: x > y)
le = Word('<=').setParseAction(lambda tokens: lambda x,y: x <= y)
ge = Word('>=').setParseAction(lambda tokens: lambda x,y: x >= y)
eq = Word('==').setParseAction(lambda tokens: lambda x,y: x == y)
ne = Word('!=').setParseAction(lambda tokens: lambda x,y: x != y)
naturalNumber = '0' | Word(srange('[1-9]'), nums)
searchRestriction = ( lt | gt | le | ge | eq | ne ) + naturalNumber
def parseRestriction(tokens):
    return Restriction(tokens[0], tokens[1])
searchRestriction.setParseAction(parseRestriction)
