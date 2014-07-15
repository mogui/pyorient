__author__ = 'Ostico <ostico@gmail.com>'

import struct

import pyorient.Messages.OrientSocket
from pyorient.utils import *
from pyorient.OrientException import *
from Constants.BinaryTypes import *


class BaseMessage(object):

    def get_orient_socket_instance(self):
        return self._orientSocket

    def is_connected(self):
        return self._session_id != -1

    def database_opened(self):
        return self._db_opened

    def __init__(self, sock=pyorient.Messages.OrientSocket):
        """
        :type sock: OrientSocket
        """
        sock.get_connection()
        self._orientSocket = sock
        self._protocol = self._orientSocket.protocol
        self._session_id = self._orientSocket.session_id

        self._header = []
        """:type : list of [str]"""

        self._body = []
        """:type : list of [str]"""

        self._fields_definition = []
        """:type : list of [object]"""

        self._command = chr(0)
        self._db_opened = self._orientSocket.db_opened
        self._output_buffer = ''
        self._input_buffer = ''

    def _update_socket_id(self):
        """Force update of socket id from inside the class"""
        self._orientSocket.session_id = self._session_id
        return self

    def _reset_fields_definition(self):
        self._fields_definition = []

    def prepare(self, *args):
        # session_id
        self._fields_definition.insert( 1, ( FIELD_INT, self._session_id ) )
        self._output_buffer = ''.join(
            self._encode_field( x ) for x in self._fields_definition
        )
        return self

    def get_protocol(self):
        if self._protocol < 0:
            self._protocol = self._orientSocket.protocol
        return self._protocol

    def _decode_header(self):

        # read header's information
        self._header = [ self._decode_field( FIELD_BYTE ),
                         self._decode_field( FIELD_INT ) ]

        # decode message errors and raise an exception
        if self._header[0]:

            # Parse the error
            exception_class = ''
            exception_message = ''

            more = self._decode_field( FIELD_BOOLEAN )

            while more:
                # read num bytes by the field definition
                exception_class += self._decode_field( FIELD_STRING )
                exception_message += self._decode_field( FIELD_STRING )
                more = self._decode_field( FIELD_BOOLEAN )

            if self.get_protocol() > 18:  # > 18 1.6-snapshot
                # read serialized version of exception thrown on server side
                # useful only for java clients
                serialized_exception = self._decode_field( FIELD_STRING )
                # trash
                del serialized_exception

            raise PyOrientCommandException(
                exception_message + " - " + exception_class, [] )

    def _decode_body(self):
        # read body
        for field in self._fields_definition:
            self._body.append( self._decode_field( field ) )

        # clear field stack
        self._reset_fields_definition()
        return self

    def _decode_all(self):
        self._decode_header()
        self._decode_body()

    def fetch_response(self, *_continue):

        try:
            if len(_continue) is not 0:
                self._body = []
                self._decode_body()
                self.dump_streams()
            # already fetched, get last results as cache info
            elif len(self._body) is 0:
                self._decode_all()
                self.dump_streams()

        except (IndexError, TypeError):
            # let the debug display the output if enabled,
            # there are only a message composition error in driver development
            pass

        return self._body

    def dump_streams(self):
        if is_debug_active():
            from pyorient.hexdump import hexdump
            print "\nRequest :"
            hexdump( self._output_buffer )
            print "\nResponse:"
            hexdump( self._input_buffer )
            print "\n"

    def append(self, field):
        """
        @type field: object
        """
        self._fields_definition.append( field )
        return self

    def __str__(self):
        from pyorient.hexdump import hexdump
        return hexdump( ''.join( map( str, self._fields_definition ) ),
                        'return' )

    def send_message(self):
        self._orientSocket.write( self._output_buffer )
        self._reset_fields_definition()
        return self

    def close(self):
        self._orientSocket.close()

    @staticmethod
    def _encode_field(field):

        # tuple with type
        t, v = field
        _content = ''

        if t['type'] == INT:
            _content = struct.pack("!i", v)
        elif t['type'] == SHORT:
            _content = struct.pack("!h", v)
        elif t['type'] == LONG:
            _content = struct.pack("!q", v)
        elif t['type'] == BOOLEAN:
            _content = chr(1) if v else chr(0)
        elif t['type'] == BYTE:
            _content = v
        elif t['type'] == BYTES:
            _content = struct.pack("!i", len(v)) + v
        elif t['type'] == STRING:
            _content = struct.pack("!i", len(v)) + v
        elif t['type'] == STRINGS:
            for s in v:
                _content += struct.pack("!i", len(s)) + s

        return _content

    def _decode_field(self, _type):

        _value = ""
        # read buffer length and decode value by field definition
        if _type['bytes'] is not None:
            _value = self._orientSocket.read( _type['bytes'] )

        # if it is a string decode first 4 Bytes as INT
        # and try to read the buffer
        if _type['type'] == STRING or _type['type'] == BYTES:

            _len = struct.unpack('!i', _value)[0]
            if _len == -1:
                _decoded_string = ''
            else:
                _decoded_string = self._orientSocket.read( _len )

            self._input_buffer += _value
            self._input_buffer += _decoded_string

            return _decoded_string

        elif _type['type'] == RECORD:

            # record_type
            record_type = self._decode_field( _type['struct'][0] )

            rid = "#" + str( self._decode_field( _type['struct'][1] ) )
            rid += ":" + str( self._decode_field( _type['struct'][2] ) )

            version = self._decode_field( _type['struct'][3] )
            content = self._decode_field( _type['struct'][4] )
            return {'rid': rid, 'record_type': record_type,
                    'content': content, 'version': version}

        elif _type['type'] == LINK:

            rid = "#" + str( self._decode_field( _type['struct'][0] ) )
            rid += ":" + str( self._decode_field( _type['struct'][1] ) )
            return rid

        else:

            self._input_buffer += _value

            if _type['type'] == BOOLEAN:
                return ord(_value) == 1
            elif _type['type'] == BYTE:
                return ord(_value)
            elif _type['type'] == CHAR:
                return _value
            elif _type['type'] == SHORT:
                return struct.unpack('!h', _value)[0]
            elif _type['type'] == INT:
                return struct.unpack('!i', _value)[0]
            elif _type['type'] == LONG:
                return struct.unpack('!q', _value)[0]
