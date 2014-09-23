__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.Messages.Constants.BinaryTypes import *
from ORecordCoder import *
from .utils import *


class RecordCreateMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( RecordCreateMessage, self ).__init__(_orient_socket)

        self._data_segment_id = -1  # default
        self._cluster_id = 0
        self._record_content = OrientRecord
        self._record_type = RECORD_TYPE_DOCUMENT
        self._mode_async = 0  # means synchronous mode

        # order matters
        self._append( ( FIELD_BYTE, RECORD_CREATE ) )

    @need_db_opened
    def prepare(self, params=None):

        try:
            # mandatory if not passed by method
            self.set_cluster_id( params[0] )

            # mandatory if not passed by method
            self._record_content = params[1]

            self.set_record_type( params[2] )  # optional

        except IndexError:
            # Use default for non existent indexes
            pass

        record = self._record_content
        if not isinstance( record, OrientRecord ):
            record = self._record_content = OrientRecord( record )

        o_record_enc = ORecordEncoder( record )

        if self.get_protocol() < 24:
            self._append( ( FIELD_INT, int(self._data_segment_id) ) )

        self._append( ( FIELD_SHORT, int(self._cluster_id) ) )
        self._append( ( FIELD_STRING, o_record_enc.getRaw() ) )
        self._append( ( FIELD_BYTE, self._record_type ) )
        self._append( ( FIELD_BOOLEAN, self._mode_async ) )

        return super( RecordCreateMessage, self ).prepare()

    def fetch_response(self):

        # skip execution in case of transaction
        if self._orientSocket.in_transaction is True:
            return self

        self._append( FIELD_LONG )  # cluster-position
        self._append( FIELD_INT )  # record-version
        result = super( RecordCreateMessage, self ).fetch_response()

        # There are some strange behaviours with protocols between 19 and 23
        # the INT ( count-of-collection-changes ) in documentation
        # is present, but don't know why,
        #
        # Not every time this INT is present!!!!
        # The next fetch too.
        #
        # So, i double check for protocol here
        # and add a socket timeout.
        if self.get_protocol() > 19:
            import socket
            try:
                self._orientSocket._socket.settimeout(0.1)
                self._append( FIELD_INT )  # count-of-collection-changes
                chng = super( RecordCreateMessage, self ).fetch_response(True)
                result.append(chng[0])
            except socket.timeoutas e:
                # socket timeout ignore
                # print e
                pass
            finally:
                self._orientSocket._socket.settimeout(None)  # reset timeout

        _changes = []
        try:
            if result[2] > 0 and self.get_protocol() > 23:

                for x in range( 0, result[2] ):
                    change = [
                        self._decode_field( FIELD_LONG ),  # (uuid-most-sig-bits:long)
                        self._decode_field( FIELD_LONG ),  # (uuid-least-sig-bits:long)
                        self._decode_field( FIELD_LONG ),  # (updated-file-id:long)
                        self._decode_field( FIELD_LONG ),  # (updated-page-index:long)
                        self._decode_field( FIELD_INT )    # (updated-page-offset:int)
                    ]
                    _changes.append( change )

        except IndexError:
            # Should not happen because of protocol check
            pass

        self._record_content.update(
            version=result[1],
            rid="#" + str(self._cluster_id) + ":" + str(result[0])
        )

        return self._record_content  # [ self._record_content, _changes ]

    def set_data_segment_id(self, data_segment_id):
        self._data_segment_id = data_segment_id
        return self

    def set_cluster_id(self, cluster_id):
        self._cluster_id = parse_cluster_id(cluster_id)
        return self

    def set_record_content(self, record):
        self._record_content = record
        return self

    def set_record_type(self, record_type ):
        if record_type in RECORD_TYPES:
            # user choice storage if present
            self._record_type = record_type
        else:
            raise PyOrientBadMethodCallException(
                record_type + ' is not a valid record type', []
            )
        return self

    def set_mode_async(self):
        self._mode_async = 1
        return self