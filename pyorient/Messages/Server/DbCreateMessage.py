__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.utils import *


class DbCreateMessage(BaseMessage):

    _db_name = ''
    _db_type = ''
    _storage_type = STORAGE_TYPE_LOCAL

    def __init__(self, _orient_socket):
        super( DbCreateMessage, self ).__init__(_orient_socket)

        self._protocol = _orient_socket.protocol
        self._session_id = _orient_socket.session_id

        # order matters
        self.append( ( FIELD_BYTE, DB_CREATE ) )

    @need_connected
    def prepare(self, params=None ):

        if isinstance( params, tuple ):
            try:
                self._db_name = params[0]
                self._db_type = params[1]
                self._storage_type = params[2]
            except IndexError:
                pass

        self.append(
            (FIELD_STRINGS, [self._db_name, self._db_type, self._storage_type])
        )
        return super( DbCreateMessage, self ).prepare()

    def fetch_response(self):
        return super( DbCreateMessage, self ).fetch_response()

    def set_db_name(self, db_name):
        self._db_name = db_name
        return self

    def set_db_type(self, db_type):
        self._db_name = db_type
        return self

    def set_storage_type(self, storage_type):
        self._storage_type = storage_type
        return self
