#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
*sshtunnel* - Initiate SSH tunnels via a remote gateway. TEST FUNCTIONS
"""

import unittest
import sshtunnel
import sshserver
import random
import threading
import socket


#SSH_PORT = random.randint(20000, 20999)
SSH_PORT = sshserver.SSH_PORT
class TestSSHConnection(unittest.TestCase):
    def setUp(self):
        server_thread = threading.Thread(target=sshserver.start_server,
                                         args=(SSH_PORT,))
        server_thread.start()

    def test_remote_side(self):
        rem_port = random.randint(21000, 30000)
        rem_host = socket.gethostbyname_ex(socket.gethostname())[0]
        print 'Trying to log in to tunnel'
        tunnel = sshtunnel.open_tunnel(gateway = 'localhost',
                                       ssh_port = SSH_PORT,
                             ssh_username = sshserver.SSH_CREDENTIALS[0],
                             ssh_password = sshserver.SSH_CREDENTIALS[1],
                             remote_bind_address_list=(rem_host, rem_port))
        tunnel.start()
#        tunnel._transport.open_channel(kind='session')
        tunnel._transport.open_session()
        self.assertEqual(tunnel.remote_bind_ports, [rem_port])
        self.assertEqual(tunnel.remote_bind_hosts, [rem_host])
#        chan.send('dir')
#        print chan.recv(1024)
        print 'Stopping tunnel'
        tunnel.stop()

    def test_local_side(self):
        loc_port = random.randint(21000, 30000)
        rem_host = '192.168.1.1'
        rem_port = 443
        print 'Trying to log in to tunnel'
        tunnel = sshtunnel.open_tunnel(gateway = 'localhost',
                                       ssh_port = SSH_PORT,
                             ssh_username = sshserver.SSH_CREDENTIALS[0],
                             ssh_password = sshserver.SSH_CREDENTIALS[1],
                             remote_bind_address_list=(rem_host, rem_port),
                             local_bind_address_list=('', loc_port))
        tunnel.start()
        tunnel._transport.open_session()
        self.assertEqual(tunnel.local_bind_ports, [loc_port])
        tunnel.stop()

if __name__ == '__main__':
    unittest.main()