__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.Messages.Constants.ClientConstants import *


class ConnectMessage(BaseMessage):

    def __init__(self, _orient_socket):
        super( ConnectMessage, self ).__init__(_orient_socket)
        self._user = ''
        self._pass = ''
        self._client_id = ''
        self._serialization_type = SERIALIZATION_DOCUMENT2CSV

        self._append( ( FIELD_BYTE, CONNECT ) )

    def prepare(self, params=None ):

        if isinstance( params, tuple ):
            try:
                self._user = params[0]
                self._pass = params[1]
                self._client_id = params[2]
                self._serialization_type = params[3]
            except IndexError:
                # Use default for non existent indexes
                pass

        if self._protocol > 21:
            #TODO Implement version 22 of the protocol
            connect_string = (FIELD_STRINGS, [self._client_id,
                                              self._serialization_type,
                                              self._user, self._pass])
        else:
            connect_string = (FIELD_STRINGS, [self._client_id,
                                              self._user, self._pass])

        self._append(
            ( FIELD_STRINGS, [NAME, VERSION] )
        )._append(
            ( FIELD_SHORT, SUPPORTED_PROTOCOL )
        )._append(
            connect_string
        )
        return super( ConnectMessage, self ).prepare()

    def fetch_response(self):
        self._append( FIELD_INT )
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
