__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from .utils import *


class DbCloseMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( DbCloseMessage, self ).__init__(_orient_socket)

        # order matters
        self._append( ( FIELD_BYTE, DB_CLOSE ) )

    @need_connected
    def prepare(self, params=None):
        return super( DbCloseMessage, self ).prepare()

    def fetch_response(self):
        super( DbCloseMessage, self ).close()
        return 0
