#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Open the sftp session to the destination
"""

import paramiko
from os.path import expanduser
from socket import timeout as socket_timeout, error as socket_error
import logging
import time


class SFTPSessionError(Exception):
    """ Base exception for SFTPSession """
    pass


class SftpSession(object):
    """
    Defines methods for opening a SFTP session to a remote system
    """
    def connect(self):
        """
        Opens a secure shell session and returns the SSH client object.
        This is the main method and expects self to have a ssharguments dict
        with all keys as described in init.
        """
        ssh_config = paramiko.SSHConfig()
        cfg_file = expanduser(self.ssh_arguments.get('ssh_configfile',
                                                     '~/.ssh/config'))

        try:
            # open the ssh config file
            ssh_config.parse(open(cfg_file, 'r'))
            # looks for information for the destination system
            hostname_info = ssh_config.lookup(self.hostname)
            # gather settings for user, port and identity file
            username = hostname_info.get('user', '')
            identityfile = hostname_info.get('identityfile', '')
            tcp_port = hostname_info.get('port', '22')
        except IOError:
            self.logger.warning('Could not read SSH configuration file: %s',
                                cfg_file)

        # if a username was specified, override configuration file's (if any)
        username = self.ssh_arguments.get('ssh_user',
                                          locals().get('username', ''))

        # if a password was not specified, send an empty string
        password = self.ssh_arguments.get('ssh_pass', '')

        # if a public key file was specified, override configuration (if found)
        identityfile = self.ssh_arguments.get('ssh_key',
                                              locals().get('identityfile',
                                                           ''))

        # if a TCP port was specified, override configuration (if found)
        self.tcp_port = int(self.ssh_arguments.get('ssh_port',
                                                   locals().get('tcp_port',
                                                                self.tcp_port)
                                                   )
                            )
        # if a ssh timeout is not specified, set it to 10 seconds
        ssh_timeout = float(self.ssh_arguments.get('ssh_timeout', 10.0))

        self.logger.debug('Establishing SFTP session: %s@%s:%s...',
                          username, self.hostname, self.tcp_port)
        client = paramiko.SSHClient()

        # Load ~/.ssh/config - like file
        try:  # This is somehow required with paramiko 1.15.2
            client.load_system_host_keys()
        except TypeError:
            self.logger.debug('ssh config file could not be loaded')

        # Automatically add new hostkeys if not found in ssh config file
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # Actually open the ssh session
            if identityfile:
                try:
                    # Disable allow_agent and look_for_keys, problems with
                    # OpenVMS if there are no pkeys matching.
                    client.connect(self.hostname,
                                   port=self.tcp_port,
                                   username=username,
                                   password=password,
                                   key_filename=identityfile,
                                   allow_agent=False,
                                   look_for_keys=False,
                                   compress=True,
                                   timeout=ssh_timeout)
                # Retry without the pkey if failure
                except paramiko.AuthenticationException:
                    client.connect(self.hostname,
                                   port=self.tcp_port,
                                   username=username,
                                   password=password,
                                   allow_agent=False,
                                   look_for_keys=False,
                                   compress=True,
                                   timeout=ssh_timeout)
            else:
                client.connect(self.hostname,
                               port=self.tcp_port,
                               username=username,
                               password=password,
                               allow_agent=False,
                               look_for_keys=False,
                               compress=True,
                               timeout=ssh_timeout)
            self.ssh_transport = client

            if self.ssh_transport:
                self.ssh_transport.sftp_session = self.ssh_transport.open_sftp(
                                                                              )

            return self.ssh_transport

        except paramiko.AuthenticationException:
            self.logger.error("Can't log in to the system, bad authentication")
            raise SFTPSessionError
        except socket_timeout:
            self.logger.error("Can't connect to host: %s after %s seconds. "
                              "Timeout.", self.hostname, ssh_timeout)
            raise SFTPSessionError
        except socket_error:
            self.logger.error('Connection refused on port %s, destination: %s',
                              tcp_port, self.hostname)
            raise SFTPSessionError
        except paramiko.ssh_exception.SSHException as _exc:
            self.logger.error("Server doesn't allow sftp or tunnel forwarding,"
                              " aborting... %s", _exc)
            raise SFTPSessionError

    def run_command(self, command):
        """ Runs the specified command over the SSH session """
        return self.ssh_transport.exec_command(command)

    def status(self):
        """ Return -1 if no SSH session is established
                    0 if SSH session is up but SFTP is down
                    1 if SSH and SFTP sessions are up
        """
        return -1 if not self.ssh_transport else 1 \
            if self.ssh_transport.sftp_session else 0

    def close(self):
        """ Close an existing SSH connection """
        self.__exit__(self.Break)    # Send Break as exception type

    def __init__(self, hostname, **ssh_arguments):
        """
        Initialize sftp session. Optional ssh argument list:
        ssh_user, ssh_pass, ssh_key, ssh_configfile, ssh_port, ssh_timeout
        Otherwise: open method will check ~/.ssh/config
        """
        self.hostname = hostname
        self.logger = ssh_arguments.pop('logger') if 'logger' in \
            ssh_arguments else logging.getLogger(__name__)
        self.ssh_arguments = ssh_arguments
        self.ssh_transport = None
        self.tcp_port = 22

    def __enter__(self):
        """
        Invoked when opening the sftp session (under with statements)
        """
        ssh_carrier = self.connect()
        # if succeeded, return the sftp_session object, else exit
        self.logger.debug('%s > SFTP %sOK', self.hostname,
                          '' if ssh_carrier.sftp_session else '*NOT* ')
        return ssh_carrier

    class Break(Exception):
        """Break out of the with statement"""
        pass

    def __exit__(self, etype, *args):
        """ Gracefully terminate SSH and SFTP connections """

        if self.ssh_transport:
            if self.ssh_transport.sftp_session:
                self.ssh_transport.sftp_session.close()
                self.logger.debug('Waiting 1/2 second to avoid RST')
                time.sleep(0.5)  # Gracefully close instead of sending RST
            self.ssh_transport.close()
        self.logger.info('Closed connection to port %s', self.tcp_port)
        if etype == self.Break:
            raise SFTPSessionError
        else:
            return None

#if __name__ == "__main__":
#   print 'Testing connection to localhost'
#   _ = SftpSession('localhost',
#                   ssh_user='',
#                   ssh_pass='')
#   print 'Done'
