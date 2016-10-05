__author__ = 'Ostico <ostico@gmail.com>'

import struct
import sys

from ..exceptions import PyOrientBadMethodCallException, \
    PyOrientCommandException, PyOrientNullRecordException
from ..otypes import OrientRecord, OrientRecordLink, OrientNode

from ..hexdump import hexdump
from ..constants import BOOLEAN, BYTE, BYTES, CHAR, FIELD_BOOLEAN, FIELD_BYTE, \
    FIELD_INT, FIELD_RECORD, FIELD_SHORT, FIELD_STRING, FIELD_TYPE_LINK, INT, \
    LINK, LONG, RECORD, SHORT, STRING, STRINGS
from ..utils import is_debug_active
from ..orient import OrientSocket
from ..serializations import OrientSerialization

class BaseMessage(object):

    def __init__(self, sock=OrientSocket):
        """
        :type sock: OrientSocket
        """
        sock.get_connection()
        self._orientSocket = sock
        self._protocol = self._orientSocket.protocol
        self._session_id = self._orientSocket.session_id

        # handles token auth
        self._auth_token = self._orientSocket.auth_token
        self._request_token = False

        self._header = []
        """:type : list of [str]"""

        self._body = []
        """:type : list of [str]"""

        self._fields_definition = []
        """:type : list of [object]"""

        self._command = chr(0)
        self._db_opened = self._orientSocket.db_opened
        self._connected = self._orientSocket.connected

        self._node_list = []
        """:type : list of [OrientNode]"""

        self._serializer = None

        self._output_buffer = b''
        self._input_buffer = b''

        # callback function for async queries
        self._callback = None

        # callback for push received from the server
        self._push_callback = None

        self._need_token = True

        global in_transaction
        in_transaction = False

    def get_serializer(self):
        """
        Lazy return of the serialization, we retrive the type from the :class: `OrientSocket <pyorient.orient.OrientSocket>` object
        :return: an Instance of the serializer suitable for decoding or encoding
        """
        if self._orientSocket.serialization_type==OrientSerialization.Binary:
            return OrientSerialization.get_impl(self._orientSocket.serialization_type,
                                                self._orientSocket._props)
        else:
            return OrientSerialization.get_impl(self._orientSocket.serialization_type)

    def get_orient_socket_instance(self):
        return self._orientSocket

    def is_connected(self):
        return self._connected is True

    def database_opened(self):
        return self._db_opened

    def get_cluster_map(self):
        """:type : list of [OrientNode]"""
        return self._node_list


    def set_session_token( self, token='' ):
        """
        :param token: Set the request to True to use the token authentication
        :type token: bool|string
        :return: self
        """
        if token != '' and token is not None:
            if type(token) is bool:
                self._request_token = token
            elif type(token) is str or type(token) is bytes:
                self._request_token = True
                self._auth_token = token
                self._db_opened = True
                self._connected = True
                self._update_socket_token()
        return self

    def get_session_token( self ):
        """
        Retrieve the session token to reuse after
        :return:
        """
        return self._auth_token

    def _update_socket_id(self):
        """Force update of socket id from inside the class"""
        self._orientSocket.session_id = self._session_id
        return self

    def _update_socket_token(self):
        """Force update of socket token from inside the class"""
        self._orientSocket.auth_token = self._auth_token
        return self

    def _reset_fields_definition(self):
        self._fields_definition = []

    def prepare(self, *args):

        # session_id
        self._fields_definition.insert( 1, ( FIELD_INT, self._session_id ) )


        """
        #  Token authentication handling
        #  we must recognize ConnectMessage and DbOpenMessage messages
        """
        if self._need_token and self._request_token is True:
            self._fields_definition.insert(
                2, ( FIELD_STRING, self._auth_token )
            )

        self._output_buffer = b''.join(
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
        if self._header[0] == 1:

            # Parse the error
            exception_class = b''
            exception_message = b''

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
                exception_class.decode( 'utf8' ),
                [ exception_message.decode( 'utf8' ) ]
            )

        elif self._header[0] == 3:
            # Push notification, Node cluster changed
            # TODO: UNTESTED CODE!!!
            # FIELD_BYTE (OChannelBinaryProtocol.PUSH_DATA);  # WRITE 3
            # FIELD_INT (Integer.MIN_VALUE);  # SESSION ID = 2^-31
            # 80: \x50 Request Push 1 byte: Push command id
            push_command_id = self._decode_field(FIELD_BYTE)
            push_message = self._decode_field( FIELD_STRING )
            _, payload = self.get_serializer().decode(push_message)
            if self._push_callback:
                self._push_callback(push_command_id, payload)

            end_flag = self._decode_field( FIELD_BYTE )

            # this flag can be set more than once
            while end_flag == 3:
                self._decode_field( FIELD_INT )  # FAKE SESSION ID = 2^-31
                op_code = self._decode_field( FIELD_BYTE )  # 80: 0x50 Request Push

                # REQUEST_PUSH_RECORD	        79
                # REQUEST_PUSH_DISTRIB_CONFIG	80
                # REQUEST_PUSH_LIVE_QUERY	    81
                if op_code == 80:
                    # for node in
                    payload = self.get_serializer().decode(
                        self._decode_field( FIELD_STRING )
                    )  # JSON WITH THE NEW CLUSTER CFG

                    # reset the nodelist
                    self._node_list = []
                    for node in payload['members']:
                        self._node_list.append( OrientNode( node ) )

                end_flag = self._decode_field( FIELD_BYTE )

            # Try to set the new session id???
            self._header[1] = self._decode_field( FIELD_INT )  # REAL SESSION ID
            pass

        from .connection import ConnectMessage
        from .database import DbOpenMessage
        """
        #  Token authentication handling
        #  we must recognize ConnectMessage and DbOpenMessage messages
            TODO: change this check avoiding cross import,
            importing a subclass in a super class is bad
        """
        if not isinstance(self, (ConnectMessage, DbOpenMessage)) \
                and self._request_token is True:
            token_refresh = self._decode_field(FIELD_STRING)
            if token_refresh != b'':
                self._auth_token = token_refresh
                self._update_socket_token()

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
        """
        # Decode header and body
        # If flag continue is set( Header already read ) read only body
        :param _continue:
        :return:
        """
        if len(_continue) is not 0:
            self._body = []
            self._decode_body()
            self.dump_streams()
        # already fetched, get last results as cache info
        elif len(self._body) is 0:
            self._decode_all()
            self.dump_streams()
        return self._body

    def dump_streams(self):
        if is_debug_active():
            if len( self._output_buffer ):
                print("\nRequest :")
                hexdump( self._output_buffer )
                # print(repr(self._output_buffer))
            if len( self._input_buffer ):
                print("\nResponse:")
                hexdump( self._input_buffer )
                # print(repr(self._input_buffer))

    def _append(self, field):
        """
        @:rtype self: BaseMessage
        @type field: object
        """
        self._fields_definition.append( field )
        return self

    def __str__(self):

        return "\n_output_buffer: \n" + hexdump( self._output_buffer, 'return' ) \
               + "\n\n_input_buffer: \n" + hexdump( self._input_buffer, 'return' )

    def send(self):
        if self._orientSocket.in_transaction is False:
            self._orientSocket.write( self._output_buffer )
            self._reset_fields_definition()
        if is_debug_active():
            self.dump_streams()
            # reset output buffer
            self._output_buffer = b""

        return self

    def close(self):
        self._orientSocket.close()

    @staticmethod
    def _encode_field(field):

        # tuple with type
        t, v = field
        _content = None

        if t['type'] == INT:
            _content = struct.pack("!i", v)
        elif t['type'] == SHORT:
            _content = struct.pack("!h", v)
        elif t['type'] == LONG:
            _content = struct.pack("!q", v)
        elif t['type'] == BOOLEAN:
            if sys.version_info[0] < 3:
                _content = chr(1) if v else chr(0)
            else:
                _content = bytes([1]) if v else bytes([0])
        elif t['type'] == BYTE:
            if sys.version_info[0] < 3:
                _content = v
            else:
                _content = bytes([ord(v)])
        elif t['type'] == BYTES:
            _content = struct.pack("!i", len(v)) + v
        elif t['type'] == STRING:
            if sys.version_info[0] >= 3:
                if isinstance(v, str):
                    v = v.encode('utf-8')
            else:
                if isinstance(v, unicode):
                    v = v.encode('utf-8')
            _content = struct.pack("!i", len(v)) + v
        elif t['type'] == STRINGS:
            _content = b''
            for s in v:
                if sys.version_info[0] >= 3:
                    if isinstance(s, str):
                        s = s.encode('utf-8')
                else:
                    if isinstance(s, unicode):
                        s = s.encode('utf-8')
                _content += struct.pack("!i", len(s)) + s

        return _content

    def _decode_field(self, _type):
        _value = b""
        # read buffer length and decode value by field definition
        if _type['bytes'] is not None:
            _value = self._orientSocket.read( _type['bytes'] )
        # if it is a string decode first 4 Bytes as INT
        # and try to read the buffer
        if _type['type'] == STRING or _type['type'] == BYTES:

            _len = struct.unpack('!i', _value)[0]
            if _len == -1 or _len == 0:
                _decoded_string = b''
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

    def _read_async_records(self):
        """
        # async-result-type byte as trailing byte of a record can be:
        # 0: no records remain to be fetched
        # 1: a record is returned as a result set
        # 2: a record is returned as pre-fetched to be loaded in client's
        #       cache only. It's not part of the result set but the client
        #       knows that it's available for later access
        """
        _status = self._decode_field( FIELD_BYTE )  # status

        while _status != 0:

            try:

                # if a callback for the cache is not set, raise exception
                if not hasattr(self._callback, '__call__'):
                    raise AttributeError()

                _record = self._read_record()

                if _status == 1:  # async record type
                    # async_records.append( _record )  # save in async
                    self._callback( _record )  # save in async
                elif _status == 2:  # cache
                    # cached_records.append( _record )  # save in cache
                    self._callback( _record )  # save in cache

            except AttributeError:
                # AttributeError: 'RecordLoadMessage' object has
                # no attribute '_command_type'
                raise PyOrientBadMethodCallException(
                    str(self._callback) + " is not a callable function", [])
            finally:
                # read new status and flush the debug buffer
                _status = self._decode_field( FIELD_BYTE )  # status

    def _read_record(self):
        """
        # The format depends if a RID is passed or an entire
            record with its content.

        # In case of null record then -2 as short is passed.

        # In case of RID -3 is passes as short and then the RID:
            (-3:short)(cluster-id:short)(cluster-position:long).

        # In case of record:
            (0:short)(record-type:byte)(cluster-id:short)
            (cluster-position:long)(record-version:int)(record-content:bytes)

        :raise: PyOrientNullRecordException
        :return: OrientRecordLink,OrientRecord
        """
        marker = self._decode_field( FIELD_SHORT )  # marker

        if marker is -2:
            raise PyOrientNullRecordException('NULL Record', [])
        elif marker is -3:
            res = OrientRecordLink( self._decode_field( FIELD_TYPE_LINK ) )
        else:
            # read record
            __res = self._decode_field( FIELD_RECORD )
            
            if self._orientSocket.serialization_type==OrientSerialization.Binary:
                class_name, data = self.get_serializer().decode(__res['content'])
            else:
                # bug in orientdb csv serialization in snapshot 2.0
                class_name, data = self.get_serializer().decode(__res['content'].rstrip())


            res = OrientRecord(
                dict(
                    __o_storage=data,
                    __o_class=class_name,
                    __version=__res['version'],
                    __rid=__res['rid']
                )
            )

        self.dump_streams()  # debug log
        self._output_buffer = b''
        self._input_buffer = b''

        return res
