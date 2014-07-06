__author__ = 'Ostico'

from BaseMessage import BaseMessage
from Fields.SendingField import SendingField
from Fields.OrientOperations import *
from Fields.OrientPrimitives import *
from pyorient.utils import *

class DbDropMessage(BaseMessage):
    _db_name = ''
    _storage_type = STORAGE_TYPE_LOCAL

    def __init__(self, _orient_socket ):
        super( DbDropMessage, self ).\
            __init__(_orient_socket)

        self._protocol = _orient_socket.protocol  # get from cache
        self._session_id = _orient_socket.session_id  # get from cache

        # order matters
        self.append( SendingField( ( BYTE, DB_DROP ) ) )
        self.append( SendingField( ( INT, self._session_id ) ) )  # session_id

    def prepare(self, params=None):

        try:
            self._db_name = params[0]
            self._storage_type = params[1]  # user choice storage if present
        except IndexError:
            # Use default for non existent indexes
            pass

        self.append( SendingField( ( STRING, self._db_name ) ) )  # db_name

        if self.get_protocol() >= 16:  # > 16 1.5-snapshot
            # custom choice server_storage_type
            self.append( SendingField( ( STRING, self._storage_type ) ) )

        return super( DbDropMessage, self ).prepare()

    @need_connected
    def fetch_response(self):
        self._set_response_header_fields()
        return super( DbDropMessage, self ).fetch_response()