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

import hashlib
from zope.interface import implements
from twisted.internet.defer import succeed, fail
from twisted.cred.checkers import ICredentialsChecker, ANONYMOUS
from twisted.cred.credentials import IUsernamePassword, IAnonymous, Anonymous
from twisted.cred.error import UnauthorizedLogin
from terane.loggers import getLogger

logger = getLogger('terane.auth.credentials')

class User(object):
    def __init__(self, name, passwd, roles):
        self.name = name
        self.passwd = passwd
        self.roles = roles

anonUser = User('_ANONYMOUS', '', ['_ANONYMOUS'])

class AnonymousOnly(object):
    """
    """
    implements(ICredentialsChecker)
    credentialInterfaces = [IAnonymous]

    def __iter__(self):
        return iter([anonUser])

    def requestAvatarId(self, credentials):
        logger.debug("authentication succeeded for user _ANONYMOUS")
        return succeed('_ANONYMOUS')

    def getUser(self, avatarId):
        return anonUser

class PasswdFile(AnonymousOnly):
    """
    """
    implements(ICredentialsChecker)
    credentialInterfaces = [IUsernamePassword,IAnonymous]

    def __init__(self, passwdfile):
        self._users = {}
        with open(passwdfile, 'r') as f:
            lines = [l.strip() for l in f.readlines()]
            lineno = 0
            try:
                for line in lines:
                    lineno += 1
                    # skip comments and blank lines
                    if line.startswith('#') or line == '':
                        continue
                    fields = [f.strip() for f in line.split(':')]
                    if len(fields) < 3:
                        raise ConfigureError("invalid user specification")
                    username = fields[0]
                    if not username.isalnum():
                        raise ConfigureError("invalid user name")
                    passwd = fields[1]
                    roles = []
                    for role in [f.strip() for f in fields[2].split(',')]:
                        if role == '' or not role.isalnum():
                            raise ConfigureError("invalid role list")
                        roles.append(role)
                    if roles == []:
                        raise ConfigureError("user must have at least one role")
                    if username in self._users:
                        raise ConfigureError("user %s is already declared" % username)
                    self._users[username] = User(username, passwd, roles)
            except ConfigureError, e:
                raise ConfigureError("error parsing %s (line %i): %s" % (passwdfile,lineno,e))
        if '_ANONYMOUS' not in self._users:
            self._users['_ANONYMOUS'] = anonUser

    def __iter__(self):
        return iter(sorted(self._users.values(), key=lambda x: x.name))

    def requestAvatarId(self, credentials):
        if isinstance(credentials, Anonymous):
            return AnonymousOnly.requestAvatarId(self, credentials)
        if credentials.username in self._users:
            user = self._users[credentials.username]
            if user.passwd == hashlib.sha256(credentials.password).hexdigest():
                logger.debug("authentication succeeded for user %s" % credentials.username)
                return succeed(user.name)
        logger.debug("authentication failed for user %s" % credentials.username)
        return fail(UnauthorizedLogin())

    def getUser(self, avatarId):
        return self._users[avatarId]
