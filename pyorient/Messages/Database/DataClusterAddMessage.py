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


    def __init__(self, _orient_socket ):
        super( DataClusterAddMessage, self ).__init__(_orient_socket)

        # order matters
        self._append( ( FIELD_BYTE, DATA_CLUSTER_ADD ) )

    @need_db_opened
    def prepare(self, params=None):

        try:
            # mandatory if not passed by method
            self._cluster_name = params[0]

            # mandatory if not passed by method
            self.set_cluster_type( params[1] )

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

    def set_cluster_name(self, _cluster_name):
        self._cluster_name = _cluster_name
        return self

    def set_cluster_type(self, _cluster_type):
        try:
            if CLUSTER_TYPES.index( _cluster_type ) is not None:
                # user choice storage if present
                self._cluster_type = _cluster_type
        except ValueError:
            raise PyOrientBadMethodCallException(
                _cluster_type + ' is not a valid cluster type', []
            )
        return self

    def set_cluster_location(self, _cluster_location):
        self._cluster_location = _cluster_location
        return self

    def set_datasegment_name(self, _datasegment_name):
        self._datasegment_name = _datasegment_name
        return self

    def set_cluster_id(self, _new_cluster_id):
        self._new_cluster_id = _new_cluster_id
        return self
