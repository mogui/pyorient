__author__ = 'Ostico'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.utils import *


class DbCloseMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( DbCloseMessage, self ).\
            __init__(_orient_socket)

        self._protocol = _orient_socket.protocol  # get from cache
        self._session_id = _orient_socket.session_id  # get from cache

        # order matters
        self.append( ( FIELD_BYTE, DB_CLOSE ) )

    @need_connected
    def prepare(self, params=None):
        return super( DbCloseMessage, self ).prepare()

    def fetch_response(self):
        super( DbCloseMessage, self ).close()
        return 0
