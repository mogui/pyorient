__author__ = 'Ostico'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.utils import *

class DbExistsMessage(BaseMessage):

    _db_name = ''
    _storage_type = STORAGE_TYPE_LOCAL

    def __init__(self, _orient_socket ):
        super( DbExistsMessage, self ).\
            __init__(_orient_socket)

        self._protocol = _orient_socket.protocol  # get from socket
        self._session_id = _orient_socket.session_id  # get from socket

        # order matters
        self.append( ( FIELD_BYTE, DB_EXIST ) )

    @need_connected
    def prepare(self, params=None):

        try:
            self._db_name = params[0]
            self._storage_type = params[1]  # user choice storage if present
        except IndexError:
            # Use default for non existent indexes
            pass

        if self.get_protocol() >= 6:
            self.append( ( FIELD_STRING, self._db_name ) )  # db_name

        if self.get_protocol() >= 16:
            # > 16 1.5-snapshot
            # custom choice server_storage_type
            self.append( ( FIELD_STRING, self._storage_type ) )

        return super( DbExistsMessage, self ).prepare()

    def fetch_response(self):
        self.append( FIELD_BOOLEAN )
        return super( DbExistsMessage, self ).fetch_response()[0]

    def set_db_name(self, db_name):
        self._db_name = db_name

    def set_storage_type(self, storage_type):
        self._storage_type = storage_type