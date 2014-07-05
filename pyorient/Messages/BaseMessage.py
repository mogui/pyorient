from Fields.ReceivingField import *
from pyorient.utils import *
from pyorient.OrientSocket import *


class BaseMessage(object):

    def __init__(self, sock):
        """
        :type sock: OrientSocket
        """
        self._socket = sock.get_connection()

        self._header = []
        """:type : list of [str]"""

        self._body = []
        """:type : list of [str]"""

        self._fields_definition = []
        """:type : list of [object]"""

        self._protocol = -1

        self._command = chr(0)

        self._out_send = ''

    def _reset_stack(self):
        self._fields_definition = []

    def prepare(self):
        self._out_send = ''.join( x.content for x in self._fields_definition )
        return self

    def get_protocol(self):
        _value = self._socket.recv( SHORT )
        self._protocol = ReceivingField.decode( SHORT, _value )
        return self._protocol

    def decode(self):

        # read header's information
        for _pos in range(2):
            field = self._fields_definition[0]
            _value = self._socket.recv( field.content )

            if field.type == BYTE:
                _value = ReceivingField.decode( BYTE, _value )
            elif field.type == INT:
                _value = ReceivingField.decode( INT, _value )

            self._header.append( _value )
            self._fields_definition = self._fields_definition[1:]

        # read body
        for field in self._fields_definition:
            _value = self._socket.recv( field.content )
            if field.type == STRING or field.type == BYTES:
                _len = ReceivingField.decode( INT, _value )
                _decoded = self._socket.recv( _len )
            else:
                _decoded = ReceivingField.decode( INT, _value )

            self._body.append( _decoded )

        return self

    def get_values(self):
        if is_debug_active():
            import hexdump
            print "\nRequest :"
            hexdump.hexdump( ''.join( map( str, self._header ) ) )
            print "\nResponse:"
            hexdump.hexdump( ''.join( map( str, self._body) ) )
            print "\n"

        return self._body

    def append(self, field):
        """
        @type field: object
        """
        self._fields_definition.append( field )
        return self

    def __str__(self):
        from pyorient.hexdump import hexdump
        return hexdump( ''.join( map( str, self._fields_definition ) ), 'return' )

    def send_message(self):
        self._socket.send( self._out_send )
        self._reset_stack()
        return self
