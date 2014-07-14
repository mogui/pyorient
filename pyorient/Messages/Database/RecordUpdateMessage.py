__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.ORecordCoder import *
from pyorient.utils import *


class RecordUpdateMessage(BaseMessage):

    _data_segment_id = -1  # default
    _cluster_id = 0
    _cluster_position = 0
    _record_content = ''

    # True:  content of record has been changed
    #        and content should be updated in storage
    # False: the record was modified but its own content has not been changed.
    #        So related collections (e.g. rid-bags) have to be updated, but
    #        record version and content should not be.
    # NOT USED before protocol 23
    _update_content = True

    # > -1 default Standard document update (version control)
    _record_version_policy = -1

    _record_type = RECORD_TYPE_DOCUMENT
    _mode_async = 0  # means synchronous mode

    def __init__(self, _orient_socket ):
        super( RecordUpdateMessage, self ).\
            __init__(_orient_socket)

        self._protocol = _orient_socket.protocol  # get from socket
        self._session_id = _orient_socket.session_id  # get from socket

        # order matters
        self.append( ( FIELD_BYTE, RECORD_UPDATE ) )

    @need_db_opened
    def prepare(self, params=None):

        try:
            # mandatory if not passed by method
            self._cluster_id = params[0]

            # mandatory if not passed by method
            self._cluster_position = params[1]

            # mandatory if not passed by method
            self._record_content = params[2]

            self._update_content = params[3]  # optional
            self._record_type = params[4]  # optional
            self._record_version_policy = params[5]  # optional
            self._mode_async = params[6]  # optional
        except IndexError:
            # Use default for non existent indexes
            pass

        record = OrientRecord( self._record_content )
        o_record_enc = ORecordEncoder(record)

        self.append( ( FIELD_SHORT, int(self._cluster_id) ) )
        self.append( ( FIELD_LONG, int(self._cluster_position) ) )

        if self._protocol >= 23:
            self.append( ( FIELD_BOOLEAN, self._update_content ) )

        self.append( ( FIELD_STRING, o_record_enc.getRaw() ) )
        self.append( ( FIELD_INT, int(self._record_version_policy) ) )
        self.append( ( FIELD_BYTE, self._record_type ) )
        self.append( ( FIELD_BOOLEAN, self._mode_async ) )

        return super( RecordUpdateMessage, self ).prepare()

    def fetch_response(self):

        self.append( FIELD_INT )  # record-version
        if self._protocol > 23:
            self.append( FIELD_INT )  # count-of-collection-changes

        return super( RecordUpdateMessage, self ).fetch_response()

    def set_data_segment_id(self, data_segment_id):
        self._data_segment_id = data_segment_id
        return self

    def set_cluster_id(self, cluster_id):
        self._cluster_id = cluster_id
        return self

    def set_cluster_position(self, _cluster_position):
        self._cluster_position = _cluster_position
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

    def set_record_version_policy(self, _policy):
        self._record_version_policy = _policy
        return self

    def set_no_update_content(self):
        self._update_content = False
        return self