__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.utils import *


class DbDropMessage(BaseMessage):
    _db_name = ''
    _storage_type = ''

    def __init__(self, _orient_socket ):
        super( DbDropMessage, self ).\
            __init__(_orient_socket)

        if self.get_protocol() > 16:  # 1.5-SNAPSHOT
            self._storage_type = STORAGE_TYPE_PLOCAL
        else:
            self._storage_type = STORAGE_TYPE_LOCAL

        # order matters
        self._append( ( FIELD_BYTE, DB_DROP ) )

    @need_connected
    def prepare(self, params=None):

        if isinstance( params, tuple ) or isinstance( params, list ):
            try:
                self._db_name = params[0]
                self.set_storage_type( params[1] )
            except IndexError:
                # Use default for non existent indexes
                pass

        self._append( ( FIELD_STRING, self._db_name ) )  # db_name

        if self.get_protocol() >= 16:  # > 16 1.5-snapshot
            # custom choice server_storage_type
            self._append( ( FIELD_STRING, self._storage_type ) )

        return super( DbDropMessage, self ).prepare()

    def fetch_response(self):
        return super( DbDropMessage, self ).fetch_response()

    def set_db_name(self, db_name):
        self._db_name = db_name
        return self

    def set_storage_type(self, storage_type):
        try:
            if STORAGE_TYPES.index( storage_type ) is not None:
                # user choice storage if present
                self._storage_type = storage_type
        except ValueError:
            raise PyOrientBadMethodCallException(
                storage_type + ' is not a valid storage type', []
            )
        return self