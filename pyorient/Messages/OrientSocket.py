__author__ = 'Ostico'

from Constants.OrientPrimitives import *
from pyorient.utils import *
import socket
import struct


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
            _value = self._socket.recv( FIELD_SHORT['bytes'] )
            self.protocol = struct.unpack('!h', _value)[0]
        except socket.error, e:
            raise PyOrientConnectionException( "Socket Error: %s" % e, e.errno )

    def close(self):
        self.host = ''
        self.port = ''
        self.protocol = -1
        self.session_id = -1
        self._socket.close()