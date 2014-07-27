__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.Commons.ORecordCoder import *
from pyorient.Commons.utils import *


class RecordLoadMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( RecordLoadMessage, self ).__init__(_orient_socket)

        self._record_id = ''
        self._fetch_plan = '*:0'
        self.cached_records = []

        # order matters
        self._append( ( FIELD_BYTE, RECORD_LOAD ) )

    @need_db_opened
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

        self._append( ( FIELD_SHORT, int(_cluster) ) )
        self._append( ( FIELD_LONG, long(_position) ) )
        self._append( ( FIELD_STRING, self._fetch_plan ) )
        self._append( ( FIELD_BYTE, "0" ) )
        self._append( ( FIELD_BYTE, "0" ) )

        return super( RecordLoadMessage, self ).prepare()

    def fetch_response(self):
        self._append( FIELD_BYTE )
        _status = super( RecordLoadMessage, self ).fetch_response()[0]

        __record = []
        _record = OrientRecord()
        if _status != 0:
            self._append( FIELD_BYTES )
            self._append( FIELD_INT )
            self._append( FIELD_BYTE )

            __record = super( RecordLoadMessage, self ).fetch_response(True)
            # bug in orientdb csv serialization in snapshot 2.0,
            # strip trailing spaces
            _record = ORecordDecoder( __record[0].rstrip() )

            cached_results = self._read_async_records()  # get cache
            self.cached_records = cached_results['cached']

        return OrientRecord(
            _record.data,
            o_class=_record.className,
            rid=self._record_id,
            version=__record[1]
        )

    def set_record_id(self, _record_id):
        self._record_id = _record_id
        return self

    def set_fetch_plan(self, _fetch_plan):
        self._fetch_plan = _fetch_plan
        return self