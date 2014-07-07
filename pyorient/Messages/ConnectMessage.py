__author__ = 'Ostico'

from BaseMessage import BaseMessage
from Constants.OrientOperations import *
from Constants.OrientPrimitives import *
from Constants.ClientConstants import *


class ConnectMessage(BaseMessage):

    def __init__(self, _orient_socket):
        super( ConnectMessage, self ).__init__(_orient_socket)
        self._user = ''
        self._pass = ''
        self._client_id = ''

        self.append( ( FIELD_BYTE, CONNECT ) )

    def prepare(self, params=None ):

        if isinstance( params, tuple ):
            try:
                self._user = params[0]
                self._pass = params[1]
                self._client_id = params[2]
            except IndexError:
                pass

        self.append(
            ( FIELD_STRINGS, [NAME, VERSION] )
        ).append(
            ( FIELD_SHORT, SUPPORTED_PROTOCOL )
        ).append(
            ( FIELD_STRINGS, [self._client_id, self._user, self._pass])
        )
        return super( ConnectMessage, self ).prepare()

    def fetch_response(self):
        self.append( FIELD_INT )
        self._session_id = super( ConnectMessage, self ).fetch_response()[0]

        # IMPORTANT needed to pass the id to other messages
        self._update_socket_id()

        return self._session_id

    def set_user(self, _user):
        self._user = _user
        return self

    def set_pass(self, _pass):
        self._pass = _pass
        return self

    def set_client_id(self, _cid):
        self._client_id = _cid
        return self
