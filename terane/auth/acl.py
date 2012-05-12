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

from terane.loggers import getLogger

logger = getLogger('terane.auth.acl')

class Permission(object):
    """
    """

    ALLOW = 1
    DENY = 2
    PASS = 3

    def __init__(self, perm, action, **refs):
        # parse the permission name
        _parts = []
        for part in perm.split('::'):
            part = part.upper()
            if not part.isalnum():
                raise ValueError("invalid permission name")
            _parts.append(part)
        self.perm = tuple(_parts)
        # set the action
        if action not in (self.ALLOW, self.DENY):
            raise ValueError("invalid permission action")
        self.action = action
        # set the object references
        self.refs = refs

    @classmethod
    def fromSpec(cls, spec):
        # check for negation
        if spec[0] == '-':
            action = Permission.DENY
            spec = spec[1:]
        else:
            action = Permission.ALLOW
        # divide the spec into permission and object refs
        perm,sep,refspec = spec.partition(';')
        # parse the object references
        refs = {}
        if refspec != '':
            for ref in [o for o in refspec.split(',') if o != '']:
                rtype,sep,rname = ref.partition('=')
                if rtype not in refs:
                    rlist = []
                else:
                    rlist = refs[rtype]
                if rlist == None:
                    pass
                elif rname == '':
                    rlist = None
                else:
                    rlist.append(rname)
                refs[rtype] = rlist
        return Permission(perm, action, **refs)

    def __str__(self):
        if self.refs == {}:
            refs = ['*']
        else:
            refs = []
            for rtype,rlist in self.refs.items():
                if rlist == None:
                    refs.append("%s=*" % rtype)
                else:
                    refs.append("%s=%s" % (rtype, ','.join(rlist)))
        if self.action == Permission.ALLOW:
            return "ALLOW %s for %s" % ('::'.join(self.perm), ' '.join(refs))
        elif self.action == Permission.DENY:
            return "DENY %s for %s" % ('::'.join(self.perm), ' '.join(refs))

    def __contains__(self, other):
        _parts = []
        for part in other.split('::'):
            part = part.upper()
            if not part.isalnum():
                raise ValueError("invalid permission name")
            _parts.append(part)
        try:
            for i in range(len(self.perm)):
                if self.perm[i] != _parts[i]:
                    return False
            return True
        except IndexError:
            return False    

class ACL(object):
    """
    """

    def __init__(self, *perms):
        self._perms = list(perms)

    def __str__(self):
        return "ACL (%s)" % '; '.join([str(p) for p in self._perms])

    def append(self, perm):
        self._perms.append(perm)

    def test(self, perm):
        for p in self._perms:
            if perm in p:
                if p.action == Permission.ALLOW:
                    return Permission.ALLOW
                elif p.action == Permission.DENY:
                    return Permission.DENY
        return Permission.PASS
