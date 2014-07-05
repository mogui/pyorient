import socket
from OrientException import PyOrientConnectionException
from utils import *


class OrientSocket(object):
    """docstring for OrientSocket"""

    def __init__(self, host, port):

        self.host = host
        self.port = port
        self._socket = ''

    def get_connection(self):
        if self._socket == '':
            self.connect()
        return self._socket

    def connect(self):
        dlog("Trying to connect...")
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._socket.connect( (self.host, self.port) )
        except socket.error, e:
            raise PyOrientConnectionException( "Socket Error: %s" % e, e.errno )