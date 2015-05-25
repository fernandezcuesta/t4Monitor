#!/usr/bin/env python
#
# This file is part of rapidssh - http://bitbucket.org/gnotaras/rapidssh/
#
# rapidssh - A set of Secure Shell (SSH) server implementations in Python
#            using Twisted.conch, part of the Twisted Framework.
#
# Copyright (c) 2010 George Notaras - http://www.g-loaded.eu
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Initially based on the sshsimpleserver.py kindly published by:
# Twisted Matrix Laboratories - http://twistedmatrix.com
#
 
import sys
import pam
 
from twisted.conch.unix import UnixSSHRealm
from twisted.cred import portal
from twisted.cred.credentials import IUsernamePassword
from twisted.cred.checkers import ICredentialsChecker
from twisted.cred.error import UnauthorizedLogin
from twisted.conch.checkers import SSHPublicKeyDatabase
from twisted.conch.ssh import factory, userauth, connection, keys, session
from twisted.internet import reactor, defer
from zope.interface import implements
from twisted.python import log
 
# Logging
# Currently logging to STDERR
log.startLogging(sys.stderr)
 
# Server-side public and private keys. These are the keys found in
# sshsimpleserver.py. Make sure you generate your own using ssh-keygen!
 
publicKey = 'ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAGEArzJx8OYOnJmzf4tfBEvLi8DVPrJ3/c9k2I/Az64fxjHf9imyRJbixtQhlH9lfNjUIx+4LmrJH5QNRsFporcHDKOTwTTYLh5KmRpslkYHRivcJSkbh/C+BR3utDS555mV'
 
privateKey = """-----BEGIN RSA PRIVATE KEY-----
MIIByAIBAAJhAK8ycfDmDpyZs3+LXwRLy4vA1T6yd/3PZNiPwM+uH8Yx3/YpskSW
4sbUIZR/ZXzY1CMfuC5qyR+UDUbBaaK3Bwyjk8E02C4eSpkabJZGB0Yr3CUpG4fw
vgUd7rQ0ueeZlQIBIwJgbh+1VZfr7WftK5lu7MHtqE1S1vPWZQYE3+VUn8yJADyb
Z4fsZaCrzW9lkIqXkE3GIY+ojdhZhkO1gbG0118sIgphwSWKRxK0mvh6ERxKqIt1
xJEJO74EykXZV4oNJ8sjAjEA3J9r2ZghVhGN6V8DnQrTk24Td0E8hU8AcP0FVP+8
PQm/g/aXf2QQkQT+omdHVEJrAjEAy0pL0EBH6EVS98evDCBtQw22OZT52qXlAwZ2
gyTriKFVoqjeEjt3SZKKqXHSApP/AjBLpF99zcJJZRq2abgYlf9lv1chkrWqDHUu
DZttmYJeEfiFBBavVYIF1dOlZT0G8jMCMBc7sOSZodFnAiryP+Qg9otSBjJ3bQML
pSTqy7c3a2AScC/YyOwkDaICHnnD3XyjMwIxALRzl0tQEKMXs6hH8ToUdlLROCrP
EhQ0wahUTCk1gKA4uPD6TMTChavbh4K63OvbKg==
-----END RSA PRIVATE KEY-----"""
 
 
class PamPasswordDatabase:
    """Authentication/authorization backend using the 'login' PAM service"""
    credentialInterfaces = IUsernamePassword,
    implements(ICredentialsChecker)
 
    def requestAvatarId(self, credentials):
        if pam.authenticate(credentials.username, credentials.password):
            return defer.succeed(credentials.username)
        return defer.fail(UnauthorizedLogin("invalid password"))
 
 
class UnixSSHdFactory(factory.SSHFactory):
    publicKeys = {
        'ssh-rsa': keys.Key.fromString(data=publicKey)
    }
    privateKeys = {
        'ssh-rsa': keys.Key.fromString(data=privateKey)
    }
    services = {
        'ssh-userauth': userauth.SSHUserAuthServer,
        'ssh-connection': connection.SSHConnection
    }
 
# Components have already been registered in twisted.conch.unix
 
portal = portal.Portal(UnixSSHRealm())
portal.registerChecker(PamPasswordDatabase())   # Supports PAM
portal.registerChecker(SSHPublicKeyDatabase())  # Supports PKI
UnixSSHdFactory.portal = portal
#factory.SSHFactory.portal = portal
 
if __name__ == '__main__':
    reactor.listenTCP(5022, UnixSSHdFactory())
#    reactor.listenTCP(5022, factory.SSHFactory())
    reactor.run()
