import struct
import socket
from OrientPrimitives import *
from OrientException import PyOrientConnectionException

import os

class OrientSocket(object):
    """docstring for OrientSocket"""

    def __init__(self, host, port):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.s.connect((host, port))
        except socket.error, e:
            raise PyOrientConnectionException( "Socket Error: %s" % e, e.errno )
        self.buffer = ''
        self.__session_id = -1
        self.__binary_buffer = ''
        self.__buffer = []

    def reset_buffer(self):
        self.__buffer = []

    def append_buffer(self, string):
        self.__buffer.append( string )

    def flush_string_buffer(self):
        self.__binary_buffer = ''.join([repr(num) for num in xrange(len(self.__buffer))])
        if 'DEBUG' in os.environ:
            if os.environ['DEBUG']:
                import sys
                if os.path.realpath( '../localTest' ) not in sys.path:
                    sys.path.insert( 0, os.path.realpath( '../localTest' ) )
                import hexdump
                hexdump.hexdump( self.__binary_buffer )


    def set_session_id(self, session_id):
        self.__session_id = session_id

    def get_session_id(self):
        return self.__session_id

    #
    # Read basic types from socket
    #
    def read_bool(self):
        return self.read_byte() == 1  # 1 = true, 0 = false

    def read_byte(self):
        return ord(self.s.recv(1))

    def read_short(self):
        return struct.unpack('!h', self.s.recv(2))[0]

    def read_int(self):
        return struct.unpack('!i', self.s.recv(4))[0]

    def read_long(self):
        return struct.unpack('!q', self.s.recv(8))[0]

    def read_bytes(self):
        l = self.read_int()
        if l == -1:
            return None
        return self.s.recv(l)

    def read_string(self):
        return self.read_bytes()

    def read_record(self):
        # TODO: implent
        raise Exception("TO implement")
        pass

    def read_strings(self):
        n = self.read_int()
        a = []
        for i in range(0, n):
            a.append(self.read_string())
        return a

    #
    # Write basic types on socketparse_status
    #
    def put_bool(self, b):
        self.buffer += chr(1) if b else chr(0)

    def put_byte(self, c):
        self.buffer += c

    def put_short(self, num):
        self.buffer += struct.pack("!h", num)

    def put_int(self, num):
        self.buffer += struct.pack("!i", num)

    def put_long(self, num):
        self.buffer += struct.pack("!q", num)

    def put_bytes(self, _bytes):
        self.put_int(len(_bytes))
        self.buffer += _bytes

    def put_string(self, string):
        self.put_bytes(string)

    def put_record(self, record):
        # TODO: implement
        raise Exception("TO implement")

    def put_strings(self, strings):
        for s in strings:
            self.put_string(s)

    #
    # Send and flush the buffer
    #
    def send(self):
        self.s.send(self.buffer)
        self.buffer = ''

    #
    # Prepare a command to be sent on the connection
    # take a list of fields, it auto-guess by the type for string and int
    # otherwise it expects a tuple with the right type
    #
    def make_request(self, operation, fields=None):

        if fields is None:
            fields = []

        # Global function
        from OrientDB import dlog
        dlog("Making request: (%d) %s" % (ord(operation), fields))

        # write operation
        self.put_byte(operation)

        # write current session
        self.put_int(self.__session_id)

        # iterate commands
        for field in fields:
            if isinstance(field, str):
                self.put_string(field)
            elif isinstance(field, int):
                self.put_int(field)
            else:
                # tuple with type
                t, v = field
                if t == SHORT:
                    self.put_short(v)
                elif t == BYTE:
                    self.put_byte(v)
        # end for

        # send command[]
        self.send()

    #
    # Parse a response from the server
    # giving back the raw content of the response
    #
    def parse_response(self, types=None):

        if types is None:
            types = []

        status, errors = self.parse_status()
        if not status:
            # null all the expected returns
            content = [None for t in types]
            return tuple([status, errors] + content)

        content = []

        for t in types:
            if t == INT:
                content.append(self.read_int())
            elif t == SHORT:
                content.append(self.read_short())
            elif t == LONG:
                content.append(self.read_long())
            elif t == BOOLEAN:
                content.append(self.read_bool())
            elif t == BYTE:
                content.append(self.read_byte())
            elif t == BYTES:
                content.append(self.read_bytes())
            elif t == STRING:
                content.append(self.read_string())
            elif t == STRINGS:
                content.append(self.read_strings())
            elif t == RECORD:
                content.append(self.read_record())

        return tuple([status, errors] + content)

    #
    # Parse status and eventual errors
    #
    def parse_status(self):
        # get status (0=OK, 1=ERROR)
        status = not self.read_bool()

        # get session id
        self.__session_id = self.read_int()
        # todo: check that session is the same??

        errors = []

        if not status:
            # Parse the error
            while self.read_bool():
                exception_class = self.read_string()
                exception_message = self.read_string()
                errors.append((exception_class, exception_message))

        return tuple([status, errors])