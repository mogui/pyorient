__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.Commons.utils import *


class CommandMessage(BaseMessage):

    def __init__(self, _orient_socket):
        super( CommandMessage, self ).__init__(_orient_socket)

        self._query = ''
        self._limit = 20
        self._fetch_plan = '*:0'
        self._command_type = QUERY_SYNC
        self._mod_byte = 's'

        self._append( ( FIELD_BYTE, COMMAND ) )

    @need_db_opened
    def prepare(self, params=None ):

        if isinstance( params, tuple ) or isinstance( params, list ):
            try:

                self.set_command_type( params[0] )

                self._query = params[1]
                self._limit = params[2]
                self._fetch_plan = params[3]

                # callback function use to operate
                # over the async fetched records
                self._callback = params[4]

            except IndexError:
                # Use default for non existent indexes
                pass

        if self._command_type == QUERY_CMD \
                or self._command_type == QUERY_SYNC \
                or self._command_type == QUERY_GREMLIN:
            self._mod_byte = 's'
        else:
            if self._callback is None:
                raise PyOrientBadMethodCallException( "No callback was "
                                                                  "provided.",[])
            self._mod_byte = 'a'

        _payload_definition = [
            ( FIELD_STRING, self._command_type ),
            ( FIELD_STRING, self._query )
        ]

        if self._command_type == QUERY_ASYNC \
                or self._command_type == QUERY_SYNC \
                or self._command_type == QUERY_GREMLIN:
            # set limit from sql string every times override the limit param
            _payload_definition.append( ( FIELD_INT, self._limit ) )
            _payload_definition.append( ( FIELD_STRING, self._fetch_plan ) )

        _payload_definition.append( ( FIELD_INT, 0 ) )

        payload = ''.join(
            self._encode_field( x ) for x in _payload_definition
        )

        self._append( ( FIELD_BYTE, self._mod_byte ) )
        self._append( ( FIELD_STRING, payload ) )

        return super( CommandMessage, self ).prepare()

    def fetch_response(self):

        # skip execution in case of transaction
        if self._orientSocket.in_transaction is True:
            return self

        # decode header only
        void = super( CommandMessage, self ).fetch_response()

        if self._command_type == QUERY_ASYNC:
            _results = self._read_async_records()
            # cache = _results['cached']
            return _results['async']
        else:
            return self._read_sync()

    def set_command_type(self, _command_type):
        if _command_type in QUERY_TYPES:
            # user choice if present
            self._command_type = _command_type
        else:
            raise PyOrientBadMethodCallException(
                _command_type + ' is not a valid command type', []
            )
        return self

    def set_fetch_plan(self, _fetch_plan):
        self._fetch_plan = _fetch_plan
        return self

    def set_query(self, _query):
        self._query = _query
        return self

    def set_limit(self, _limit):
        self._limit = _limit
        return self

    def _read_sync(self):

        # type of response
        # decode body char with flag continue ( Header already read )
        response_type = self._decode_field( FIELD_CHAR )

        res = []
        if response_type == 'n':
            return None
        elif response_type == 'r':
            res = [ self._read_record() ]
            self._append( FIELD_CHAR )
            # end Line \x00
            _res = super( CommandMessage, self ).fetch_response(True)
        elif response_type == 'a':
            self._append( FIELD_STRING )
            self._append( FIELD_CHAR )
            res = [ super( CommandMessage, self ).fetch_response(True)[0] ]
        elif response_type == 'l':
            self._append( FIELD_INT )
            list_len = super( CommandMessage, self ).fetch_response(True)[0]

            for n in range(0, list_len):
                res.append( self._read_record() )

            # async-result-type can be:
            # 0: no records remain to be fetched
            # 1: a record is returned as a result set
            # 2: a record is returned as pre-fetched to be loaded in client's
            #       cache only. It's not part of the result set but the client
            #       knows that it's available for later access
            cached_results = self._read_async_records()
            # cache = cached_results['cached']
        else:
            msg = ""
            import pyorient.Commons.hexdump
            import socket
            self._orientSocket._socket.settimeout(5)
            try:
                m = self._orientSocket.read(1)
                while m != "":
                    msg += m
                    m = self._orientSocket.read(1)
            except socket.timeout, e:
                print "************* " + str(e) + " *************"
                pass
            pyorient.Commons.hexdump.hexdump(msg)
            exit(1)

        return res

    def set_callback(self, func):
        if hasattr(func, '__call__'):
            self._callback = func
        else:
            raise PyOrientBadMethodCallException( func + " is not a callable "
                                                         "function", [])
        return self