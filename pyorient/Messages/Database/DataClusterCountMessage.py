__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.utils import *


class DataClusterCountMessage(BaseMessage):

    _cluster_ids = []
    _count_tombstones = 0

    def __init__(self, _orient_socket ):
        super( DataClusterCountMessage, self ).\
            __init__(_orient_socket)

        self._protocol = _orient_socket.protocol  # get from cache
        self._session_id = _orient_socket.session_id  # get from cache

        # order matters
        self._append( ( FIELD_BYTE, DATA_CLUSTER_COUNT ) )

    @need_db_opened
    def prepare(self, params=None):

        try:
            # mandatory if not passed by method
            self._cluster_ids = params

            self._append( ( FIELD_SHORT, len(self._cluster_ids) ) )
            for x in self._cluster_ids:
                self._append( ( FIELD_SHORT, x ) )

            self._append( ( FIELD_BOOLEAN, self._count_tombstones ) )

        except( IndexError, TypeError ):
            # Use default for non existent indexes
            pass

        return super( DataClusterCountMessage, self ).prepare()

    def fetch_response(self):
        self._append( FIELD_LONG )
        return super( DataClusterCountMessage, self ).fetch_response()[0]

    def set_cluster_ids(self, _cluster_ids):
        self._cluster_ids = _cluster_ids
        return self

    def set_count_tombstones(self, _count_tombstones):
        self._count_tombstones = _count_tombstones
        return self
