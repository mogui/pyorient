__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.Commons.utils import *


class DbCreateMessage(BaseMessage):

    _db_name = ''
    _db_type = ''
    _storage_type = ''

    def __init__(self, _orient_socket):
        super( DbCreateMessage, self ).__init__(_orient_socket)

        if self.get_protocol() > 16:  # 1.5-SNAPSHOT
            self._storage_type = STORAGE_TYPE_PLOCAL
        else:
            self._storage_type = STORAGE_TYPE_LOCAL

        # order matters
        self._append( ( FIELD_BYTE, DB_CREATE ) )

    @need_connected
    def prepare(self, params=None ):

        if isinstance( params, tuple ) or isinstance( params, list ):
            try:
                self._db_name = params[0]
                self.set_db_type( params[1] )
                self.set_storage_type( params[2] )
            except IndexError:
                pass

        self._append(
            (FIELD_STRINGS, [self._db_name, self._db_type, self._storage_type])
        )
        return super( DbCreateMessage, self ).prepare()

    def fetch_response(self):
        super( DbCreateMessage, self ).fetch_response()
        # set database opened
        self._orientSocket.db_opened = self._db_name
        return

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
