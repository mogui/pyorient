__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.utils import *


class DataClusterDropMessage(BaseMessage):

    _cluster_id = 0
    _count_tombstones = 0

    def __init__(self, _orient_socket ):
        super( DataClusterDropMessage, self ).__init__(_orient_socket)

        # order matters
        self._append( ( FIELD_BYTE, DATA_CLUSTER_DROP ) )

    @need_db_opened
    def prepare(self, params=None):

        if isinstance( params, int ):
            # mandatory if not passed by method
            self._cluster_id = params

        self._append( ( FIELD_SHORT, self._cluster_id ) )
        return super( DataClusterDropMessage, self ).prepare()

    def fetch_response(self):
        self._append( FIELD_BOOLEAN )
        return super( DataClusterDropMessage, self ).fetch_response()[0]

    def set_cluster_id(self, _cluster_id):
        self._cluster_id = _cluster_id
        return self