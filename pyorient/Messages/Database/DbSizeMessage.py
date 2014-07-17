__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.utils import *


class DbSizeMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( DbSizeMessage, self ).__init__(_orient_socket)

        # order matters
        self._append( ( FIELD_BYTE, DB_SIZE ) )

    @need_db_opened
    def prepare(self, params=None):
        return super( DbSizeMessage, self ).prepare()

    def fetch_response(self):
        self._append( FIELD_LONG )
        return super( DbSizeMessage, self ).fetch_response()[0]
