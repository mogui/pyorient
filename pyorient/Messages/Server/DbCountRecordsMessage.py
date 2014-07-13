__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.utils import *


class DbCountRecordsMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( DbCountRecordsMessage, self ).\
            __init__(_orient_socket)

        self._user = ''
        self._pass = ''

        self._protocol = _orient_socket.protocol  # get from cache
        self._session_id = _orient_socket.session_id  # get from cache

        # order matters
        self.append( ( FIELD_BYTE, DB_COUNTRECORDS ) )

    @need_connected
    def prepare(self, params=None):
        return super( DbCountRecordsMessage, self ).prepare()

    def fetch_response(self):
        self.append( FIELD_LONG )
        return super( DbCountRecordsMessage, self ).fetch_response()[0]
