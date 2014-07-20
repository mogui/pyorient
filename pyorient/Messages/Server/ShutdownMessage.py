__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.Commons.utils import *


class ShutdownMessage(BaseMessage):

    _user = ''
    _pass = ''

    def __init__(self, _orient_socket ):
        super( ShutdownMessage, self ).\
            __init__(_orient_socket)

        # order matters
        self._append( ( FIELD_BYTE, SHUTDOWN ) )

    @need_connected
    def prepare(self, params=None):

        if isinstance( params, tuple ) or isinstance( params, list ):
            try:
                self._user = params[0]
                self._pass = params[1]
            except IndexError:
                # Use default for non existent indexes
                pass

        self._append( (FIELD_STRINGS, [self._user, self._pass]) )

        return super( ShutdownMessage, self ).prepare()

    def fetch_response(self):
        return super( ShutdownMessage, self ).fetch_response()

    def set_user(self, _user):
        self._user = _user
        return self

    def set_pass(self, _pass):
        self._pass = _pass
        return self