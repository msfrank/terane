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

from zope.interface import Interface, implements
from twisted.cred.portal import Portal
from terane.manager import IManager, Manager
from terane.settings import ConfigureError
from terane.auth.credentials import PasswdFile, AnonymousOnly
from terane.auth.acl import ACL, Permission
from terane.loggers import getLogger

logger = getLogger('terane.auth')

class IAuthManager(Interface):
    def getPortal(realm):
        """
        Return a Portal integrating the specified realm with the credentials loaded
        into the authorization manager.

        :param realm: The realm.
        :type realm: An object providing :class:`twisted.cred.portal.IRealm`
        :returns: The portal.
        :rtype: :class:`twisted.cred.portal.Portal`
        """
    def canAccess(userName, objectType, objectName, *perms):
        """
        Returns a boolean indicating whether or not the specified userName has the
        specified permissions on the specified object.

        :param userName: The user identifier.
        :type userName: str
        :param objectType: The object type name.
        :type objectType: str
        :param objectName: The object name.
        :type objectName: str
        :param perms: The list of permission names.
        :type perms: list
        """

class AuthManager(Manager):
    """
    The AuthManager loads authentication and authorization data and provides methods
    to validate users and mediate access to objects.
    """

    implements(IManager, IAuthManager)

    def __init__(self):
        Manager.__init__(self)
        self._creds = None
        self._roles = {}

    def configure(self, settings):
        section = settings.section("server")
        if section.getBoolean("disable auth", False) == True:
            logger.warning("auth is disabled!")
            self._creds = AnonymousOnly()
            self._roles = {'_ANONYMOUS': ACL(Permission('PERM', Permission.ALLOW))}
        else:
            # load users db
            passwdfile = section.getPath("auth password file", "/etc/terane/passwd")
            logger.debug("loading users from %s" % passwdfile)
            self._creds = PasswdFile(passwdfile)
            # load roles db
            rolesfile = section.getPath("auth roles file", "/etc/terane/roles")
            self._loadRoles(rolesfile)
            # if no _ANONYMOUS role was specified, then create a deny-all ACL
            if not '_ANONYMOUS' in self._roles:
                self._roles['_ANONYMOUS'] = ACL(Permission('PERM', Permission.DENY))
        for user in self._creds:
            logger.debug("user %s => Role (%s)" % (user.name,', '.join(user.roles)))
        for role,acl in self._roles.items():
            logger.debug("role %s => %s" % (role,acl))

    def _loadRoles(self, rolesfile):
        logger.debug("loading roles from %s" % rolesfile)
        with open(rolesfile, 'r') as f:
            lineno = 0
            current = None
            try:
                for line in f.readlines():
                    lineno += 1
                    if line == '' or line.isspace() or line.strip().startswith('#'):
                        continue
                    # line is the starting of a declaration
                    if not line[0].isspace():
                        role,permissions = [s.strip() for s in line.split(':', 1)]
                        self._parseRole(role, permissions)
                        current = role
                    # otherwise the line is a continuation
                    else:
                        self._parseRole(current, line.strip())
            except ConfigureError, e:
                raise ConfigureError("error parsing %s (line %i): %s" % (permfile,lineno,e))

    def _parseRole(self, role, permissions):
        if not role in ('_ANONYMOUS') and not role.isalnum():
            raise Exception("invalid role name '%s'" % role)
        if role in self._roles:
            acl = self._roles[role]
        else:
            acl = ACL()
        for p in [p.strip() for p in permissions.split() if p.strip() != '']:
            acl.append(Permission.fromSpec(p))
        self._roles[role] = acl

    def getPortal(self, realm):
        return Portal(realm, [self._creds])

    def canAccess(self, userName, objectType, objectName, *perms):
        user = self._creds.getUser(userName)
        logger.debug("user %s has roles %s" % (userName, ', '.join(user.roles)))
        acls = [self._roles[r] for r in user.roles]
        for perm in perms:
            def _check(_perm):
                for acl in acls:
                    result = acl.test(_perm)
                    if result == Permission.ALLOW:
                        logger.debug("test %s for %s on %s=%s => ALLOW" % (
                            perm, userName, objectType, objectName))
                        return True
                    if result == Permission.DENY:
                        logger.debug("test %s for %s on %s=%s => DENY" % (
                            perm, userName, objectType, objectName))
                        return False
                return False
            if _check(perm) == False:
                return False
        return True
