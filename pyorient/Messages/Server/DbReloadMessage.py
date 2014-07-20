__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.Commons.utils import *


class DbReloadMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( DbReloadMessage, self ).__init__(_orient_socket)

        # order matters
        self._append( ( FIELD_BYTE, DB_RELOAD ) )

    @need_connected
    def prepare(self, params=None):
        return super( DbReloadMessage, self ).prepare()

    def fetch_response(self):

        self._append( FIELD_SHORT )  # cluster_num

        cluster_num = super( DbReloadMessage, self ).fetch_response()[0]

        clusters = []
        try:
            for x in range(0, cluster_num ):
                if self.get_protocol() < 24:
                    cluster = {
                        "name": self._decode_field( FIELD_STRING ),  # cluster_name
                        "id": self._decode_field( FIELD_SHORT ),  # cluster_id
                        "type": self._decode_field( FIELD_STRING ),  # cluster_type
                        "segment": self._decode_field( FIELD_SHORT ),  # cluster release
                    }
                else:
                    cluster = {
                        "name": self._decode_field( FIELD_STRING ),  # cluster_name
                        "id": self._decode_field( FIELD_SHORT ),  # cluster_id
                    }
                clusters.append( cluster )

        except IndexError:
            # Should not happen because of protocol check
            pass

        return clusters