__author__ = 'Ostico'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.OrientTypes import OrientRecord


class CommandMessage(BaseMessage):

    def __init__(self, _orient_socket):

        self._query = ''
        self._limit = 20
        self._fetch_plan = '*:0'
        self._command_type = QUERY_SYNC
        self._mod_byte = 's'

        super( CommandMessage, self ).__init__(_orient_socket)

        self.append( ( FIELD_BYTE, COMMAND ) )

    def prepare(self, params=None ):

        if isinstance( params, tuple ) or isinstance( params, list ):
            try:
                self._command_type = params[0]
                self._query = params[1]
                self._limit = params[2]
                self._fetch_plan = params[3]

                if params[0] is QUERY_CMD \
                        or params[0] is QUERY_SYNC \
                        or params[0] is QUERY_GREMLIN:
                    self._mod_byte = 's'
                else:
                    self._mod_byte = 'a'

            except IndexError:
                # Use default for non existent indexes
                pass

        _payload_definition = [
            ( FIELD_STRING, self._command_type ),
            ( FIELD_STRING, self._query )
        ]

        if self._command_type is QUERY_ASYNC \
                or self._command_type is QUERY_SYNC \
                or self._command_type is QUERY_GREMLIN:
            # set limit to -1 and get it from sql string
            _payload_definition.append( ( FIELD_INT, self._limit ) )
            _payload_definition.append( ( FIELD_STRING, self._fetch_plan ) )

        _payload_definition.append( ( FIELD_INT, 0 ) )

        payload = ''.join(
            self._encode_field( x ) for x in _payload_definition
        )

        self.append(
            ( FIELD_BYTE, self._mod_byte )
        ).append(
            ( FIELD_STRING, payload )
        )

        return super( CommandMessage, self ).prepare()

    def fetch_response(self):

        self.append( FIELD_CHAR )  # type of response

        response_type = super( CommandMessage, self ).fetch_response()[0]

        self._reset_fields_definition()

        res = []
        if response_type == 'n':
            raise NotImplementedError
        elif response_type == 'r':
            raise NotImplementedError
        elif response_type == 'l':
            self.append( FIELD_INT )
            list_len = super( CommandMessage, self ).fetch_response(True)[0]
            self._reset_fields_definition()
            for n in range(0, list_len):
                self.append( FIELD_RECORD )

            res = super( CommandMessage, self ).fetch_response(True)

        return res

    def set_async(self, _sync_type):
        self._command_type = _sync_type
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