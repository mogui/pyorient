__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.Server.ConnectMessage import *
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Commons.utils import *


class DbOpenMessage(BaseMessage):

    _user = ''
    _pass = ''
    _client_id = ''
    _db_name = ''
    _db_type = DB_TYPE_DOCUMENT
    _serialization_type = SERIALIZATION_DOCUMENT2CSV

    def __init__(self, _orient_socket):
        super( DbOpenMessage, self ).__init__(_orient_socket)
        self._append( ( FIELD_BYTE, DB_OPEN ) )

    def _perform_connection(self):
        # try to connect, we inherited BaseMessage
        conn_message = ConnectMessage( self._orientSocket )
        # set session id and protocol
        self._session_id = conn_message\
            .prepare( ( self._user, self._pass, self._client_id ) )\
            .fetch_response()
        # now, self._session_id and _orient_socket.session_id are updated
        self.get_protocol()

    def prepare(self, params=None ):

        if isinstance( params, tuple ) or isinstance( params, list ):
            try:
                self._db_name = params[0]
                self._user = params[1]
                self._pass = params[2]

                self.set_db_type( params[3] )

                self._client_id = params[4]

                self.set_serialization_type( params[5] )

            except IndexError:
                # Use default for non existent indexes
                pass

        # if session id is -1, so we aren't connected
        # because ConnectMessage set the client id
        # this block of code check for session because this class
        # can be initialized directly from orient socket
        if self._orientSocket.session_id < 0:
            self._perform_connection()

        if self.get_protocol() > 21:
            connect_string = (FIELD_STRINGS, [self._client_id,
                                              self._serialization_type,
                                              self._db_name,
                                              self._db_type,
                                              self._user, self._pass])
        else:
            connect_string = (FIELD_STRINGS, [self._client_id,
                                              self._db_name, self._db_type,
                                              self._user, self._pass])

        self._append( ( FIELD_STRINGS, [NAME, VERSION] ) )
        self._append( ( FIELD_SHORT, SUPPORTED_PROTOCOL ) )
        self._append( connect_string )

        return super( DbOpenMessage, self ).prepare()

    def fetch_response(self):
        self._append( FIELD_INT )  # session_id
        self._append( FIELD_SHORT )  # cluster_num

        self._session_id, cluster_num = \
            super( DbOpenMessage, self ).fetch_response()

        clusters = []
        try:
            for x in range(0, cluster_num ):
                if self.get_protocol() < 24:
                    cluster = {
                        "name": self._decode_field( FIELD_STRING ),  # cluster_name
                        "id": self._decode_field( FIELD_SHORT ),  # cluster_id
                        "type": self._decode_field( FIELD_STRING ),  # cluster_type
                        "segment": self._decode_field( FIELD_SHORT ),  # cluster release
                    }
                else:
                    cluster = {
                        "name": self._decode_field( FIELD_STRING ),  # cluster_name
                        "id": self._decode_field( FIELD_SHORT ),  # cluster_id
                    }
                clusters.append( cluster )

        except IndexError:
            # Should not happen because of protocol check
            pass

        self._append( FIELD_INT )  # cluster config string ( -1 )
        self._append( FIELD_STRING )  # cluster release

        response = super( DbOpenMessage, self ).fetch_response(True)

        # set database opened
        self._orientSocket.db_opened = self._db_name

        return clusters

    def set_db_name(self, db_name):
        self._db_name = db_name
        return self

    def set_db_type(self, db_type):
        try:
            if DB_TYPES.index( db_type ) is not None:
                self._db_type = db_type
        except ValueError:
            raise PyOrientBadMethodCallException(
                db_type + ' is not a valid database type', []
            )
        return self

    def set_client_id(self, _cid):
        self._client_id = _cid
        return self

    def set_user(self, _user):
        self._user = _user
        return self

    def set_pass(self, _pass):
        self._pass = _pass
        return self

    def set_serialization_type(self, serialization_type):
        #TODO Implement version 22 of the protocol
        if serialization_type == SERIALIZATION_SERIAL_BIN:
            raise NotImplementedError

        try:
            if SERIALIZATION_TYPES.index( serialization_type ) is not None:
                # user choice storage if present
                self._serialization_type = serialization_type
        except ValueError:
            raise PyOrientBadMethodCallException(
                serialization_type + ' is not a valid serialization type', []
            )
        return self