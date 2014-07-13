__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.ORecordCoder import *
from pyorient.utils import *


class RecordLoadMessage(BaseMessage):

    _record_id = ''
    _fetch_plan = '*:0'

    cached_records = {}

    def __init__(self, _orient_socket ):
        super( RecordLoadMessage, self ).\
            __init__(_orient_socket)

        self._protocol = _orient_socket.protocol  # get from socket
        self._session_id = _orient_socket.session_id  # get from socket

        # order matters
        self.append( ( FIELD_BYTE, RECORD_LOAD ) )

    @need_connected
    def prepare(self, params=None):

        try:
            self._record_id = params[0]  # mandatory if not passed with set
            self._fetch_plan = params[1]  # user choice if present
        except IndexError:
            # Use default for non existent indexes
            pass

        _cluster, _position = self._record_id.split( ':' )
        if _cluster[0] is '#':
            _cluster = _cluster[1:]

        self.append( ( FIELD_SHORT, int(_cluster) ) )
        self.append( ( FIELD_LONG, long(_position) ) )
        self.append( ( FIELD_STRING, self._fetch_plan ) )
        self.append( ( FIELD_BYTE, "0" ) )
        self.append( ( FIELD_BYTE, "0" ) )

        return super( RecordLoadMessage, self ).prepare()

    def fetch_response(self):
        self.append( FIELD_BYTE )
        _status = super( RecordLoadMessage, self ).fetch_response()[0]
        self._reset_fields_definition()

        _record = OrientRecord()

        if _status != 0:
            self.append( FIELD_BYTES )
            self.append( FIELD_INT )
            self.append( FIELD_BYTE )

            __record = super( RecordLoadMessage, self ).fetch_response(True)[0]
            _record = ORecordDecoder( __record )

            self._reset_fields_definition()

            self.append( FIELD_BYTE )  # status
            _status = super( RecordLoadMessage, self ).fetch_response(True)[0]

            self._reset_fields_definition()

            cached_records = {}
            while _status != 0:

                marker = self._decode_field( FIELD_SHORT )  # status

                if marker is -2:
                    return None
                elif marker is -3:
                    self.append( FIELD_TYPE_LINK )
                    return super( RecordLoadMessage, self ).fetch_response(True)

                # cached Records, not used at moment.
                # moreover, ORecordDecoder can't handle the document data types
                # so, perform a raw read and flush the debug buffer
                # before starting a new loop
                cached_record = self._decode_field( FIELD_RECORD )

                cached_records.__setitem__(
                    cached_record['rid'],
                    cached_record
                )  # save in cache

                # read new status and flush the debug buffer
                self.append( FIELD_BYTE )  # status
                _status = super( RecordLoadMessage, self ).fetch_response(True)[0]

                self._reset_fields_definition()
                self._output_buffer = ''
                self._input_buffer = ''

            self.cached_records = cached_records

        return OrientRecord(_record.data, o_class=_record.className,
                            rid=self._record_id)

    def set_db_name(self, _record_id):
        self._record_id = _record_id
        return self

    def set_storage_type(self, _fetch_plan):
        self._fetch_plan = _fetch_plan
        return self