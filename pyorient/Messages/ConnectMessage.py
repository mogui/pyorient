from BaseMessage import BaseMessage
from Fields.SendingField import SendingField
from Fields.ReceivingField import ReceivingField
from Fields.OrientOperations import *
from Fields.OrientPrimitives import *
from Fields.ClientConstants import *


class ConnectMessage(BaseMessage):

    def __init__(self, sock):
        super( ConnectMessage, self ).__init__(sock)
        self.append( SendingField( ( BYTE, CONNECT ) ) )
        self.append( SendingField( ( INT, -1 ) ) )
        self._user = ''
        self._pass = ''
        self._client_id = ''

    def prepare(self):
        self.append( SendingField(
            ( STRINGS, [NAME, VERSION] ) )
        ).append(
            SendingField( ( SHORT, SUPPORTED_PROTOCOL ) )
        ).append(
            SendingField( ( STRINGS, [self._client_id, self._user, self._pass] ) )
        )
        return super( ConnectMessage, self ).prepare()

    def decode(self):

        status = ReceivingField( BYTE )
        empty_sid = ReceivingField( INT )
        sid = ReceivingField( INT )
        self.append( status ).append( empty_sid ).append( sid )

        return super( ConnectMessage, self ).decode()

    def set_user(self, _user):
        self._user = _user
        return self

    def set_pass(self, _pass):
        self._pass = _pass
        return self

    def set_client_id(self, _cid):
        self._client_id = _cid
        return self