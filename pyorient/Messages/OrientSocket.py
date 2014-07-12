__author__ = 'Ostico'

from Constants.OrientPrimitives import *
from pyorient.utils import *
import socket
import struct

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


class OrientSocket(object):
    """docstring for OrientSocket"""

    MAX_READ_LENGTH = 512000

    def __init__(self, host, port):

        self._connected = False
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
            self._connected = True
        except socket.error, e:
            raise PyOrientConnectionException( "Socket Error: %s" % e, e.errno )

    def close(self):
        self.host = ''
        self.port = ''
        self.protocol = -1
        self.session_id = -1
        self._socket.close()

    def write(self, buff):
        return self._socket.send(buff)

    # The man page for recv says: The receive calls normally return
    #   any data available, up to the requested amount, rather than waiting
    #   for receipt of the full amount requested.
    #
    # If you need to read a given number of bytes, you need to call recv
    #   in a loop and concatenate the returned packets until
    #   you have read enough.
    def read(self, _len_to_read):

        buf = StringIO()
        try:

            while buf.tell() < _len_to_read:

                if is_debug_active():
                    tmp = self._socket.recv( _len_to_read - buf.tell() )
                    buf.write( tmp )
                    import pyorient.hexdump
                    print( "\n          -------------------\n" )
                    print( "    To read was: " + str( _len_to_read ) )
                    print( pyorient.hexdump.hexdump(tmp, 'return') )
                    print( "    left: " + str( _len_to_read - buf.tell() ) )

                else:
                    buf.write( self._socket.recv( _len_to_read - buf.tell() ) )

            buf.seek(0)
            return buf.read( _len_to_read )
        except Exception, e:
            raise e
        finally:
            buf.close()