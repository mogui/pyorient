__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.utils import *


class DataClusterAddMessage(BaseMessage):

    _cluster_name     = ''
    _cluster_type     = CLUSTER_TYPE_PHYSICAL
    _cluster_location = 'default'
    _datasegment_name = 'default'
    _new_cluster_id   = -1

    _cluster_type_range = [
        CLUSTER_TYPE_PHYSICAL,
        CLUSTER_TYPE_MEMORY
    ]

    def __init__(self, _orient_socket ):
        super( DataClusterAddMessage, self ).\
            __init__(_orient_socket)

        self._protocol = _orient_socket.protocol  # get from cache
        self._session_id = _orient_socket.session_id  # get from cache

        # order matters
        self._append( ( FIELD_BYTE, DATA_CLUSTER_ADD ) )

    @need_db_opened
    def prepare(self, params=None):

        try:
            # mandatory if not passed by method
            self._cluster_name = params[0]

            if self._cluster_type_range.index( params[1] ):
                # mandatory if not passed by method
                self._cluster_type = params[1]

            self._cluster_location = params[2]
            self._datasegment_name = params[3]

        except( IndexError, TypeError ):
            # Use default for non existent indexes
            pass
        except ValueError:
            raise PyOrientBadMethodCallException(
                params[1] + ' is not a valid data cluster type', []
            )

        self._append( ( FIELD_STRING, self._cluster_type ) )
        self._append( ( FIELD_STRING, self._cluster_name ) )
        self._append( ( FIELD_STRING, self._cluster_location ) )
        self._append( ( FIELD_STRING, self._datasegment_name ) )

        if self._protocol >= 18:
            self._append( ( FIELD_SHORT, self._new_cluster_id ) )

        return super( DataClusterAddMessage, self ).prepare()

    def fetch_response(self):
        self._append( FIELD_SHORT )
        return super( DataClusterAddMessage, self ).fetch_response()[0]

    def set_cluster_ids(self, _new_cluster_id):
        self._new_cluster_id = _new_cluster_id
        return self

