__author__ = 'Ostico'


import socket
from OrientException import PyOrientConnectionException
from Messages.Fields.ReceivingField import *
from utils import *


class OrientSocket(object):
    """docstring for OrientSocket"""

    def __init__(self, host, port):

        self.host = host
        self.port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        """:type : socket.socket"""
        self.protocol = -1
        self.session_id = -1

    def get_connection(self):
        try:
            if self._socket.connect_ex((self.host, self.port)) is not 106:
                self.connect()
        except socket.error, e:
            # catch socket Exception 'Bad file descriptor' if connection closed
            self.connect()

        return self._socket

    def connect(self):
        dlog("Trying to connect...")
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect( (self.host, self.port) )
            _value = self._socket.recv( SHORT )
            self.protocol = ReceivingField.decode( SHORT, _value )
        except socket.error, e:
            raise PyOrientConnectionException( "Socket Error: %s" % e, e.errno )

    def close(self):
        self.host = ''
        self.port = ''
        self.protocol = -1
        self.session_id = -1
        self._socket.close()