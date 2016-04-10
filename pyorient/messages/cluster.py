# -*- coding: utf-8 -*-

__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.exceptions import PyOrientBadMethodCallException

from .base import BaseMessage
from ..constants import CLUSTER_TYPE_PHYSICAL, DATA_CLUSTER_ADD_OP, \
    DATA_CLUSTER_COUNT_OP, FIELD_BOOLEAN, FIELD_BYTE, FIELD_LONG, FIELD_SHORT, \
    FIELD_STRING, DATA_CLUSTER_DATA_RANGE_OP, DATA_CLUSTER_DROP_OP, CLUSTER_TYPES
from ..utils import need_db_opened


#
# DATACLUSTER ADD
#
# Add a new data cluster.
#
# Request: (name:string)(cluster-id:short - since 1.6 snapshot)
# Response: (new-cluster:short)
#
# Where: type is one of "PHYSICAL" or "MEMORY". If cluster-id is -1 (recommended value)
#   new cluster id will be generated.
#
class DataClusterAddMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( DataClusterAddMessage, self ).__init__(_orient_socket)

        self._cluster_name     = ''
        self._cluster_type     = CLUSTER_TYPE_PHYSICAL
        self._cluster_location = 'default'
        self._datasegment_name = 'default'
        self._new_cluster_id   = -1

        # order matters
        self._append( ( FIELD_BYTE, DATA_CLUSTER_ADD_OP ) )

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

        if self.get_protocol() < 24:
            self._append( ( FIELD_STRING, self._cluster_type ) )
            self._append( ( FIELD_STRING, self._cluster_name ) )
            self._append( ( FIELD_STRING, self._cluster_location ) )
            self._append( ( FIELD_STRING, self._datasegment_name ) )
        else:
            self._append( ( FIELD_STRING, self._cluster_name ) )

        if self.get_protocol() >= 18:
            self._append( ( FIELD_SHORT, self._new_cluster_id ) )

        return super( DataClusterAddMessage, self ).prepare()

    def fetch_response(self):
        self._append( FIELD_SHORT )
        return super( DataClusterAddMessage, self ).fetch_response()[0]

    def set_cluster_name(self, _cluster_name):
        self._cluster_name = _cluster_name
        return self

    def set_cluster_type(self, _cluster_type):
        if _cluster_type in CLUSTER_TYPES:
            # user choice storage if present
            self._cluster_type = _cluster_type
        else:
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

#
# DATA CLUSTER COUNT
#
# Returns the number of records in one or more clusters.
#
# Request: (cluster-count:short)(cluster-number:short)*(count-tombstones:byte)
# Response: (records-in-clusters:long)
# Where:
#
# cluster-count the number of requested clusters
# cluster-number the cluster id of each single cluster
# count-tombstones the flag which indicates whether deleted records
#   should be taken in account. It is applicable for autosharded storage only,
#   otherwise it is ignored.
# records-in-clusters is the total number of records found in the requested clusters
#
class DataClusterCountMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( DataClusterCountMessage, self ).__init__(_orient_socket)

        self._cluster_ids = []
        self._count_tombstones = 0

        # order matters
        self._append( ( FIELD_BYTE, DATA_CLUSTER_COUNT_OP ) )

    @need_db_opened
    def prepare(self, params=None):

        if isinstance( params, tuple ) or isinstance( params, list ):
            try:
                # mandatory if not passed by method
                # raise Exception if None
                if isinstance( params[0], tuple ) or isinstance( params[0], list ):
                    self._cluster_ids = params[0]
                else:
                    raise PyOrientBadMethodCallException(
                        "Cluster IDs param must be an instance of Tuple or List.", []
                    )

                self._count_tombstones = params[1]
            except( IndexError, TypeError ):
                # Use default for non existent indexes
                pass

        self._append( ( FIELD_SHORT, len(self._cluster_ids) ) )
        for x in self._cluster_ids:
            self._append( ( FIELD_SHORT, x ) )

        self._append( ( FIELD_BOOLEAN, self._count_tombstones ) )

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


#
# DATA CLUSTER DATA RANGE
#
# Returns the range of record ids for a cluster.
#
# Request: (cluster-number:short)
# Response: (begin:long)(end:long)
#
class DataClusterDataRangeMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( DataClusterDataRangeMessage, self ).__init__(_orient_socket)

        self._cluster_id = 0
        self._count_tombstones = 0

        # order matters
        self._append( ( FIELD_BYTE, DATA_CLUSTER_DATA_RANGE_OP ) )

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

#
# DATA CLUSTER DROP
#
# Remove a cluster.
#
# Request: (cluster-number:short)
# Response: (delete-on-clientside:byte)
#
class DataClusterDropMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( DataClusterDropMessage, self ).__init__(_orient_socket)

        self._cluster_id = 0
        self._count_tombstones = 0

        # order matters
        self._append( ( FIELD_BYTE, DATA_CLUSTER_DROP_OP ) )

    @need_db_opened
    def prepare(self, params=None):

        if isinstance( params[0], int ):
            # mandatory if not passed by method
            self._cluster_id = params[0]

        self._append( ( FIELD_SHORT, self._cluster_id ) )
        return super( DataClusterDropMessage, self ).prepare()

    def fetch_response(self):
        self._append( FIELD_BOOLEAN )
        return super( DataClusterDropMessage, self ).fetch_response()[0]

    def set_cluster_id(self, _cluster_id):
        self._cluster_id = _cluster_id
        return self