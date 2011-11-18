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

__version__ = (0, 1, 4)

def versionstring():
    """
    Return the version number as a string.
    """
    return "%i.%i.%i" % __version__

# add the lib/ directory to the module search path
import sys
from os.path import abspath, dirname, join
sys.path.insert(0, abspath(join(dirname(__file__), 'lib')))
