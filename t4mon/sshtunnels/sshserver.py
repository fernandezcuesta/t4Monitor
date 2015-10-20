import select
import socket
import sys
import threading

import paramiko

HOST_KEY = paramiko.RSAKey(filename='key.key')
SSH_PORT = 22022
SSH_CREDENTIALS = ('test', 'testing123')

class Server (paramiko.ServerInterface):
    def __init__(self):
        self.event = threading.Event()


    def check_channel_request(self, kind, chanid):
        print "Incoming request for channel type %s." % kind
        if kind == 'session':
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_auth_password(self, username, password):
        if (username == SSH_CREDENTIALS[0]) and (password == SSH_CREDENTIALS[1]):
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED
    def check_channel_pty_request(self, *args):
#        self.logger.debug("Got the PTY request, pretending to accept it.",
#                          extra={'username' : self.transport.get_username()})
        print 'pty requested'
        return True

    def check_channel_shell_request(self, channel):
        # Shell isn't our target, so log as debug
        print "Got shell request. Denying it: %s" % self.transport.get_username()
        channel.send(self.no_shell)
        # Don't let the GC delete the channel. For further details, see above.
        self.session_channel = channel
        return True


def start_server(ssh_port=SSH_PORT):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('127.0.0.1', ssh_port))
        sock.listen(100)
        print '[+] Listening for connection ...'
        client, addr = sock.accept()
    except Exception, e:
        print '[-] Listen/bind/accept failed: ' + str(e)
        sys.exit(1)
    print '[+] Got a connection!'
     
    try:
        t = paramiko.Transport(client)
        try:
            t.load_server_moduli()
        except:
            print '[-] (Failed to load moduli -- gex will be unsupported.)'
            raise
        t.add_server_key(HOST_KEY)
        server = Server()
        
        try:
            t.start_server(server=server)
        except paramiko.SSHException:
            print '[-] SSH negotiation failed.'
        chan = t.accept(5)
        print '[+] Authenticated! Now we should just echo UPPERCASE'
        while True:
            data = chan.recv(1024)
            if len(data) == 0:
                break
            chan.send(data.upper())
#        while True:
#            r, w, x = select.select([sock, chan], [], [])
#            if sock in r:
#                data = sock.recv(1024)
#                if len(data) == 0:
#                    break
#                chan.send(data)
#
#            if chan in r:
#                data = chan.recv(1024)
#                if len(data) == 0:
#                    break
#                sock.send(data)        
        chan.close()
        sock.close()
        
    except Exception as e:
        print '[-] Caught exception: ', repr(e)
        try:
            t.close()
        except:
            pass
    
if __name__ == '__main__':
    print '[+] Trying to create server on port %s ...' % SSH_PORT
    start_server()
    print 'SERVER STOPPED'
