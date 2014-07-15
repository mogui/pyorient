__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Server.ConnectMessage import *
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *


class DbOpenMessage(BaseMessage):

    def __init__(self, _orient_socket):

        self._user = ''
        self._pass = ''
        self._client_id = ''
        self._db_name = ''
        self._db_type = DB_TYPE_DOCUMENT
        self._serialization_type = SERIALIZATION_DOCUMENT2CSV

        super( DbOpenMessage, self ).__init__(_orient_socket)
        # this block of code check for session because this class
        # can be initialized directly from orient socket

        self.append( ( FIELD_BYTE, DB_OPEN ) )

    def _perform_connection(self):
        # try to connect, we inherited BaseMessage
        conn_message = ConnectMessage( self._orientSocket )
        # set session id and protocol
        self._session_id = conn_message\
            .prepare( ( self._user, self._pass, self._client_id ) )\
            .send_message().fetch_response()
        # now, self._session_id and _orient_socket.session_id are updated
        self._protocol = self._orientSocket.protocol

    def prepare(self, params=None ):

        if isinstance( params, tuple ):
            try:
                self._user = params[0]
                self._pass = params[1]
                self._client_id = params[2]
                self._db_name = params[3]
                self._db_type = params[4]
                self._serialization_type = params[5]
            except IndexError:
                # Use default for non existent indexes
                pass

        # if session id is -1, so we aren't connected
        # because ConnectMessage set the client id
        if self._orientSocket.session_id < 0:
            self._perform_connection()

        if self._protocol > 21:
            connect_string = (FIELD_STRINGS, [self._client_id,
                                              self._serialization_type,
                                              self._db_name,
                                              self._db_type,
                                              self._user, self._pass])
        else:
            connect_string = (FIELD_STRINGS, [self._client_id,
                                              self._db_name, self._db_type,
                                              self._user, self._pass])

        self.append(
            ( FIELD_STRINGS, [NAME, VERSION] )
        ).append(
            ( FIELD_SHORT, SUPPORTED_PROTOCOL )
        ).append(
            connect_string
        )
        return super( DbOpenMessage, self ).prepare()

    def fetch_response(self):
        self.append( FIELD_INT )  # session_id
        self.append( FIELD_SHORT )  # cluster_num

        self._session_id, cluster_num = \
            super( DbOpenMessage, self ).fetch_response()

        for n in range(0, cluster_num):
            self.append( FIELD_STRING )  # cluster_name
            self.append( FIELD_SHORT )  # cluster_id
            self.append( FIELD_STRING )  # cluster_type
            self.append( FIELD_SHORT )  # cluster_segment_id

        self.append( FIELD_INT )  # cluster config string ( -1 )
        self.append( FIELD_STRING )  # cluster release

        response = super( DbOpenMessage, self ).fetch_response(True)

        clusters = []
        for n in range(0, cluster_num):
            x = n * 4
            cluster_name = response[x]
            cluster_id = response[x + 1]
            cluster_type = response[x + 2]
            cluster_segment_data_id = response[x + 3]
            clusters.append({
                "name": cluster_name,
                "id": cluster_id,
                "type": cluster_type,
                "segment": cluster_segment_data_id
            })

        # set database opened
        self._orientSocket.db_opened = self._db_name

        return clusters

    def set_db_name(self, db_name):
        self._db_name = db_name
        return self

    def set_db_type(self, db_type):
        self._db_name = db_type
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