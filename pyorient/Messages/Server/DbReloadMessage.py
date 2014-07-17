__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.utils import *


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

        for n in range(0, cluster_num):
            self._append( FIELD_STRING )  # cluster_name
            self._append( FIELD_SHORT )  # cluster_id
            self._append( FIELD_STRING )  # cluster_type
            self._append( FIELD_SHORT )  # cluster_segment_id

        response = super( DbReloadMessage, self ).fetch_response(True)

        clusters = []
        for n in range(0, cluster_num):
            x = n * 4
            cluster_name = response[x]
            cluster_id = response[x + 1]
            cluster_type = response[x + 2]
            cluster_segment_data_id = response[x + 3]
            clusters.append({
                "name": cluster_name,
                "id": cluster_id,
                "type": cluster_type,
                "segment": cluster_segment_data_id
            })

        return clusters