__author__ = 'Ostico'

from BaseMessage import BaseMessage
from Fields.SendingField import SendingField
from Fields.ReceivingField import ReceivingField
from Fields.OrientOperations import *
from Fields.OrientPrimitives import *
from Fields.ClientConstants import *


class ConnectMessage(BaseMessage):

    def __init__(self, _orient_socket):
        super( ConnectMessage, self ).__init__(_orient_socket)
        self._user = ''
        self._pass = ''
        self._client_id = ''

        self.append( SendingField( ( BYTE, CONNECT ) ) )
        # session_id = -1
        self.append( SendingField( ( INT, self._session_id ) ) )

    def prepare(self, params=None ):

        if isinstance( params, tuple ):
            try:
                self._user = params[0]
                self._pass = params[1]
                self._client_id = params[2]
            except IndexError:
                pass

        self.append(
            SendingField( ( STRINGS, [NAME, VERSION] ) )
        ).append(
            SendingField( ( SHORT, SUPPORTED_PROTOCOL ) )
        ).append(
            SendingField( (STRINGS, [self._client_id, self._user, self._pass]) )
        )
        return super( ConnectMessage, self ).prepare()

    def fetch_response(self):
        self._set_response_header_fields()
        self.append( ReceivingField( INT ) )
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
