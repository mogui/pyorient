__author__ = 'Ostico'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.OrientPrimitives import *
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
                self.append( FIELD_RECORD )  # cached Record
                cached_records.__setitem__(
                    self._record_id, super( RecordLoadMessage, self )
                    .fetch_response(True) )  # save in cache

                self._reset_fields_definition()
                self.append( FIELD_BYTE )  # status
                _status = super( RecordLoadMessage, self ).fetch_response(True)[0]
                self._reset_fields_definition()

            self.cached_records = cached_records

        return OrientRecord(_record.data,
                            o_class=_record.className, rid=self._record_id)

    def set_db_name(self, _record_id):
        self._record_id = _record_id
        return self

    def set_storage_type(self, _fetch_plan):
        self._fetch_plan = _fetch_plan
        return self