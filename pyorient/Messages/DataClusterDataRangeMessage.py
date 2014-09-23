__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from .utils import *


class DataClusterDataRangeMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( DataClusterDataRangeMessage, self ).__init__(_orient_socket)

        self._cluster_id = 0
        self._count_tombstones = 0

        # order matters
        self._append( ( FIELD_BYTE, DATA_CLUSTER_DATA_RANGE ) )

    @need_db_opened
    def prepare(self, params=None):

        if isinstance( params, int ):
            # mandatory if not passed by method
            self._cluster_id = params

        self._append( ( FIELD_SHORT, self._cluster_id ) )
        return super( DataClusterDataRangeMessage, self ).prepare()

    def fetch_response(self):
        self._append( FIELD_LONG )
        self._append( FIELD_LONG )
        return super( DataClusterDataRangeMessage, self ).fetch_response()

    def set_cluster_id(self, _cluster_id):
        self._cluster_id = _cluster_id
        return self