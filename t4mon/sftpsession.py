#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Open the sftp session to the destination
"""

import time
import getpass
import logging
from socket import error as socket_error
from socket import timeout as socket_timeout
from os.path import expanduser

import paramiko
from sshtunnel import HandlerSSHTunnelForwarderError


class SFTPSessionError(Exception):

    """ Base exception for SFTPSession """
    pass


class SftpSession(object):

    """
    Defines methods for opening a SFTP session to a remote OpenVMS system
    """

    def __init__(self, hostname, **ssh_arguments):
        """
        Initialize sftp session. Optional ssh argument list:
        ssh_user, ssh_pass, ssh_key, ssh_configfile, ssh_port, ssh_timeout
        Otherwise: open method will check ~/.ssh/config
        """
        # Remove all "None" input values
        list(map(ssh_arguments.pop,
                 [item for item in ssh_arguments if not ssh_arguments[item]])
             )
        self.hostname = self.address = hostname
        self.logger = ssh_arguments.pop('logger') if 'logger' in \
            ssh_arguments else logging.getLogger(__name__)
        self.ssh_arguments = ssh_arguments
        self.ssh_transport = None
        self.tcp_port = 22

    def __enter__(self):
        """
        Invoked when opening the sftp session (under with statements)
        """
        sftp_session = self.connect()
        sftp_session.run_command = self.run_command
        sftp_session.ssh_transport = self.ssh_transport
        # if succeeded, return the sftp_session object, else exit
        self.logger.debug('%s > SFTP %sOK', self.hostname,
                          '' if sftp_session else '*NOT* ')
        return sftp_session

    class Break(Exception):

        """Break out of the with statement"""
        pass

    def __exit__(self, etype, *args):
        """ Gracefully terminate SSH and SFTP connections """

        if self.ssh_transport:
            if self.ssh_transport.sftp_session:
                self.ssh_transport.sftp_session.close()
                self.logger.debug('Waiting 1/2 second to avoid RST')
                time.sleep(0.3)  # Gracefully close instead of sending RST
            self.ssh_transport.close()
        self.logger.info('Closed connection to port %s', self.tcp_port)
        if etype == self.Break:
            raise SFTPSessionError
        else:
            return None

    def connect(self):
        """
        Opens a secure shell session and returns the SSH client object.
        This is the main method and expects self to have a ssharguments dict
        with all keys as described in init.
        """
        (username, identityfile, self.tcp_port) = self._load_ssh_config()
        # if a password was not specified, send an empty string
        password = self.ssh_arguments.get('ssh_pass', '')
        # if a ssh timeout is not specified, set it to 10 seconds
        ssh_timeout = float(self.ssh_arguments.get('ssh_timeout', 10.0))

        self.logger.debug('Establishing SFTP session: %s@%s:%s...',
                          username, self.hostname, self.tcp_port)
        client = paramiko.SSHClient()

        try:
            client.load_system_host_keys()
            # Automatically add new hostkeys if not found in ssh config file
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        except TypeError:
            self.logger.debug('ssh known hosts file could not be loaded')

        try:
            # Actually open the ssh session
            if identityfile:
                try:
                    # Disable allow_agent and look_for_keys, problems with
                    # OpenVMS if there are no pkeys matching.
                    client.connect(self.address,
                                   port=self.tcp_port,
                                   username=username,
                                   password=password,
                                   key_filename=expanduser(identityfile),
                                   allow_agent=False,
                                   look_for_keys=False,
                                   compress=True,
                                   timeout=ssh_timeout)
                # Retry without the password if failure
                except paramiko.AuthenticationException:
                    client.connect(self.address,
                                   port=self.tcp_port,
                                   username=username,
                                   password=password,
                                   allow_agent=False,
                                   look_for_keys=False,
                                   compress=True,
                                   timeout=ssh_timeout)
            else:
                client.connect(self.address,
                               port=self.tcp_port,
                               username=username,
                               password=password,
                               allow_agent=False,
                               look_for_keys=False,
                               compress=True,
                               timeout=ssh_timeout)
            self.ssh_transport = client

            if self.ssh_transport:
                self.ssh_transport.sftp_session = \
                    self.ssh_transport.open_sftp()
                return self.ssh_transport.sftp_session
            else:
                return None

        except paramiko.AuthenticationException:
            self.logger.error("Can't log in to the system, bad authentication")
            raise SFTPSessionError
        except socket_timeout:
            self.logger.error("Can't connect to host: %s after %s seconds. "
                              "Timeout.", self.hostname, ssh_timeout)
            raise SFTPSessionError
        except socket_error:
            self.logger.error('Connection refused on port %s, destination: %s',
                              self.tcp_port, self.hostname)
            raise SFTPSessionError
        except paramiko.ssh_exception.SSHException as _exc:
            self.logger.error("Server doesn't allow sftp or tunnel forwarding,"
                              " aborting... %s", _exc)
            raise SFTPSessionError
        except HandlerSSHTunnelForwarderError as _exc:
            self.logger.error('Something went wrong with the SSH '
                              'transport: %s', repr(_exc))

    def run_command(self, command):
        """
        Run the specified command over the SSH session and return (clean)
        stdout
        """
        (_, stdout, _) = self.ssh_transport.exec_command(command)
        return stdout.readlines()

    def status(self):
        """
        Return -1 if no SSH session is established
                0 if SSH session is up but SFTP is down
                1 if SSH and SFTP sessions are up
        """
        try:
            return -1 if not self.ssh_transport._transport else 1 \
                if self.ssh_transport.sftp_session else 0
        except AttributeError:
            return -1

    def open(self):
        """ Close an initialised SFTP connection """
        return self.__enter__()

    def close(self):
        """ Close an existing SFTP connection """
        return self.__exit__(None)

    def _load_ssh_config(self):
        """
        Check ~/.ssh/config for username, port or identityfile, return values
        passed as arguments if not found.
        """
        ssh_config = paramiko.SSHConfig()
        # Load ~/.ssh/config - like file
        cfg_file = expanduser(self.ssh_arguments.get('ssh_configfile',
                                                     '~/.ssh/config'))
        try:
            # open the ssh config file
            ssh_config.parse(open(cfg_file, 'r'))
            # looks for information for the destination system
            hostname_info = ssh_config.lookup(self.hostname)
        except IOError:
            self.logger.warning('Could not read SSH configuration file: %s',
                                cfg_file)
            hostname_info = {}
        # gather settings for user, port and identity file
        username = hostname_info.get('user',
                                     getpass.getuser())  # Current username
        identityfile = hostname_info.get('identityfile', [''])[0]
        tcp_port = hostname_info.get('port', '22')
        # update hostname address
        self.address = hostname_info.get('hostname', self.hostname)
        # if a username was specified, override settings (if any)
        username = self.ssh_arguments.get('ssh_user',
                                          locals().get('username'))
        # if a public key file was specified, override settings (if found)
        identityfile = self.ssh_arguments.get(
            'ssh_key',
            locals().get('identityfile', '')
        )
        # if a TCP port was specified, override configuration (if found)
        self.tcp_port = int(self.ssh_arguments.get(
            'ssh_port',
            locals().get('tcp_port', self.tcp_port))
        )
        return (username, identityfile, self.tcp_port)
