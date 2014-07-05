__author__ = 'Ostico'

from Fields.ReceivingField import *
from Fields.SendingField import *
from pyorient.OrientSocket import *
from pyorient.OrientException import *


#
# need connection decorator
# def need_session(wrap):
#     def wrap_function(*args, **kwargs):
#         if args[0].session_id < 0:
#             raise PyOrientBadMethodCallException(
#                 "Session Id not provided or not valid. Call set_session_id", [])
#         return wrap(*args, **kwargs)
#
#     return wrap_function


class BaseMessage(object):

    def get_orient_socket_instance(self):
        return self._orientSocket

    def __init__(self, sock=OrientSocket):
        """
        :type sock: OrientSocket
        """
        self._socket = sock.get_connection()

        self._orientSocket = sock

        self._header = []
        """:type : list of [str]"""

        self._body = []
        """:type : list of [str]"""

        self._fields_definition = []
        """:type : list of [object]"""

        self._protocol = -1
        self._command = chr(0)
        self._output_buffer = ''
        self._input_buffer = ''

        self._session_id = -1

    def set_session_id(self, session_id=-1):
        self._session_id = session_id
        self.append( SendingField( ( INT, self._session_id ) ) )
        return self

    def _set_response_header_fields(self):
        status = ReceivingField( BYTE )
        sid = ReceivingField( INT )
        self.append( status ).append( sid )
        return self

    def _reset_fields_definition(self):
        self._fields_definition = []

    def prepare(self, *args):
        self._output_buffer = ''.join( x.content for x in self._fields_definition )
        return self

    def get_protocol(self):
        if self._protocol < 0:
            self._protocol = self._orientSocket.protocol
        return self._protocol

    def _decode_header(self):
        # read header's information
        for _pos in range(2):
            field = self._fields_definition[0]
            _value = self._socket.recv( field.content )

            if field.type == BYTE:
                value = ReceivingField.decode( BYTE, _value )
            elif field.type == INT:
                value = ReceivingField.decode( INT, _value )
            else:
                raise PyOrientBadMethodCallException( "Invalid Header Field "
                                                      + field )

            self._header.append( value )
            self._fields_definition = self._fields_definition[1:]
            self._input_buffer += _value

        # decode message errors and raise an exception
        if self._header[0]:
            self._reset_fields_definition()

            # Parse the error
            _bool_field = ReceivingField( BOOLEAN )
            _str_field = ReceivingField( STRING )

            exception_class = ''
            exception_message = ''

            more = ReceivingField.decode(
                BOOLEAN, self._socket.recv( _bool_field.content ) )

            while more:
                # read num bytes by the field definition
                _len = ReceivingField.decode(
                    INT, self._socket.recv( _str_field.content ) )
                exception_class += self._socket.recv( _len )

                # read num bytes by the field definition
                _len = ReceivingField.decode(
                    INT, self._socket.recv( _str_field.content ) )
                exception_message += self._socket.recv( _len )

                more = ReceivingField.decode(
                    BOOLEAN, self._socket.recv( _bool_field.content ) )

            if self.get_protocol() > 18:  # > 18 1.6-snapshot
                # read serialized version of exception thrown on server side
                # useful only for java clients
                _len = ReceivingField.decode(
                    INT, self._socket.recv( _str_field.content ) )
                serialized_exception = self._socket.recv( _len )  # trash

            raise PyOrientCommandException(
                exception_message + " - " + exception_class, [] )

    def _decode_body(self):
        # read body
        for field in self._fields_definition:

            # read num bytes by the field definition
            _value = self._socket.recv( field.content )

            # if it is a string decode first 4 Bytes as INT
            # and try to read the buffer
            if field.type == STRING or field.type == BYTES:
                _len = ReceivingField.decode( INT, _value )
                if _len == -1:
                    _decoded = ''
                else:
                    _decoded = self._socket.recv( _len )

                self._input_buffer += _value
                self._input_buffer += _decoded

            else:
                # read buffer length and decode value by field definition
                _decoded = ReceivingField.decode( field.type, _value )
                self._input_buffer += _value

            self._body.append( _decoded )

        self._reset_fields_definition()
        return self

    def _decode(self):
        self._decode_header()
        self._decode_body()

    def fetch_response(self, *_continue):

        log = False

        try:
            if len(_continue) is not 0:
                self._body = []
                self._decode_body()
                log = True
            # already fetched, get last results as cache info
            elif len(self._body) is 0:
                self._decode()
                log = True

        except (IndexError, TypeError), e:
            # let the debug display the output if enabled,
            # there are only a message composition error in driver development
            pass

        # if len(_continue) is not 0 or len(self._body) is 0:
        if log is True:
            if is_debug_active():
                from pyorient.hexdump import hexdump
                print "\nRequest :"
                hexdump( self._output_buffer )
                print "\nResponse:"
                hexdump( self._input_buffer )
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
        self._socket.send( self._output_buffer )
        self._reset_fields_definition()
        return self

    def close(self):
        self._socket.close()