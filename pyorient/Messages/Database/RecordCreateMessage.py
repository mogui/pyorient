__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.ORecordCoder import *
from pyorient.utils import *


class RecordCreateMessage(BaseMessage):

    _data_segment_id = -1  # default
    _cluster_id = 0
    _record_content = ''
    _record_type = RECORD_TYPE_DOCUMENT
    _mode_async = 0  # means synchronous mode

    def __init__(self, _orient_socket ):
        super( RecordCreateMessage, self ).\
            __init__(_orient_socket)

        self._protocol = _orient_socket.protocol  # get from socket
        self._session_id = _orient_socket.session_id  # get from socket

        # order matters
        self.append( ( FIELD_BYTE, RECORD_CREATE ) )

    @need_connected
    def prepare(self, params=None):

        try:
            # mandatory if not passed by method
            self._cluster_id = params[0]

            # mandatory if not passed by method
            self._record_content = params[1]

            self._record_type = params[2]  # optional
        except IndexError:
            # Use default for non existent indexes
            pass

        record = OrientRecord( self._record_content )
        o_record_enc = ORecordEncoder(record)

        self.append( ( FIELD_INT, int(self._data_segment_id) ) )
        self.append( ( FIELD_SHORT, int(self._cluster_id) ) )
        self.append( ( FIELD_STRING, o_record_enc.getRaw() ) )
        self.append( ( FIELD_BYTE, self._record_type ) )
        self.append( ( FIELD_BOOLEAN, self._mode_async ) )

        return super( RecordCreateMessage, self ).prepare()

    def fetch_response(self):

        self.append( FIELD_LONG )  # cluster-position
        self.append( FIELD_INT )  # record-version

        return super( RecordCreateMessage, self ).fetch_response()

    def set_data_segment_id(self, data_segment_id):
        self._data_segment_id = data_segment_id
        return self

    def set_cluster_id(self, cluster_id):
        self._cluster_id = cluster_id
        return self

    def set_record_content(self, record):
        self._record_content = record
        return self

    def set_record_type(self, record_type ):
        self._record_type = record_type
        return self

    def set_mode_async(self):
        self._mode_async = 1
        return self