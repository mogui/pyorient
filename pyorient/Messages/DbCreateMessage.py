__author__ = 'Ostico'

from BaseMessage import BaseMessage
from Fields.SendingField import SendingField
from Fields.OrientOperations import *
from Fields.OrientPrimitives import *


class DbCreateMessage(BaseMessage):

    _db_name = ''
    _db_type = ''
    _storage_type = STORAGE_TYPE_LOCAL

    def __init__(self, conn_message):
        super( DbCreateMessage, self ).\
            __init__(conn_message.get_orient_socket_instance())

        self._protocol = conn_message.get_protocol()
        self._session_id = conn_message.fetch_response()

        # order matters
        self.append( SendingField( ( BYTE, DB_CREATE ) ) )
        self.append( SendingField( ( INT, self._session_id ) ) )  # session_id

    def prepare(self, params=None ):

        if isinstance( params, tuple ):
            try:
                self._db_name = params[0]
                self._db_type = params[1]
                self._storage_type = params[2]
            except IndexError:
                pass

        self.append(
            SendingField( (STRINGS, [self._db_name, self._db_type, self._storage_type]) )
        )
        return super( DbCreateMessage, self ).prepare()

    def fetch_response(self):
        self._set_response_header_fields()
        return super( DbCreateMessage, self ).fetch_response()

    def set_db_name(self, db_name):
        self._db_name = db_name

    def set_db_type(self, db_type):
        self._db_name = db_type

    def set_storage_type(self, storage_type):
        self._storage_type = storage_type
