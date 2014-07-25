__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.Commons.utils import *


class RecordDeleteMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( RecordDeleteMessage, self ).__init__(_orient_socket)

        self._cluster_id = 0
        self._cluster_position = 0
        self._record_version = -1
        self._mode_async = 0  # means synchronous mode

        # only needed for transactions
        self._record_type = RECORD_TYPE_DOCUMENT

        # order matters
        self._append( ( FIELD_BYTE, RECORD_DELETE ) )

    @need_db_opened
    def prepare(self, params=None):

        try:
            # mandatory if not passed by method
            self.set_cluster_id( params[0] )

            # mandatory if not passed by method
            self.set_cluster_position( params[1] )

            self._record_version = params[2]   # optional
            self._mode_async = params[3]  # optional
        except IndexError:
            # Use default for non existent indexes
            pass

        self._append( ( FIELD_SHORT, int(self._cluster_id) ) )
        self._append( ( FIELD_LONG, int(self._cluster_position) ) )
        self._append( ( FIELD_INT, int(self._record_version) ) )
        self._append( ( FIELD_BOOLEAN, self._mode_async ) )

        return super( RecordDeleteMessage, self ).prepare()

    def fetch_response(self):
        self._append( FIELD_BOOLEAN )  # payload-status
        return super( RecordDeleteMessage, self ).fetch_response()[0]

    def set_record_version(self, _record_version):
        self._record_version = _record_version
        return self

    def set_cluster_id(self, cluster_id):
        self._cluster_id = parse_cluster_id(cluster_id)
        return self

    def set_cluster_position(self, _cluster_position):
        self._cluster_position = parse_cluster_position(_cluster_position)
        return self

    def set_record_type(self, _record_type):
        self._record_type = _record_type
        return self

    def set_mode_async(self):
        self._mode_async = 1
        return self
