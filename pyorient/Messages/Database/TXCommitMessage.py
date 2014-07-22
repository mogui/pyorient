__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.Messages.Database.RecordUpdateMessage import RecordUpdateMessage
from pyorient.Messages.Database.RecordCreateMessage import RecordCreateMessage
from pyorient.Messages.Database.RecordDeleteMessage import RecordDeleteMessage
from pyorient.Commons.utils import *
from pyorient.Commons.ORecordCoder import *
import inspect


class TXCommitMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super( TXCommitMessage, self ).__init__(_orient_socket)

        self.tx_id = -1
        self._operation_stack = []

        self._temp_cluster_position_seq = -2

        # order matters
        self._append( ( FIELD_BYTE, TX_COMMIT ) )
        self._command = TX_COMMIT

    @need_connected
    def prepare(self, params=None):

        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        if calframe[2][3] != 'commit':
            raise AttributeError(
                "type object '" + self.__class__.__name__ +
                "' has no attribute 'prepare'"
            )

        self._append( ( FIELD_INT, self.get_transaction_id() ) )
        self._append( ( FIELD_BOOLEAN, True ) )

        for k, v in enumerate(self._operation_stack):
            self._append( ( FIELD_BYTE, chr(1) ) )  # start of records
            map( self._append, v )

        self._append( ( FIELD_BYTE, chr(0) ) )
        self._append( ( FIELD_STRING, "" ) )

        return super( TXCommitMessage, self ).prepare()

    def fetch_response(self):
        # self.dump_streams()

        super(TXCommitMessage, self).fetch_response()

        result = {
            'created': [],
            'updated': [],
            'changes': []
        }

        items = self._decode_field( FIELD_INT )
        for x in range( 0, items ):

            # (created-record-count:int)
            # [
            #     (client-specified-cluster-id:short)
            #     (client-specified-cluster-position:long)
            #     (created-cluster-id:short)
            #     (created-cluster-position:long)
            # ]*
            result['created'].append(
                {
                    'client_c_id': self._decode_field(FIELD_SHORT),
                    'client_c_pos': self._decode_field(FIELD_LONG),
                    'created_c_id': self._decode_field(FIELD_SHORT),
                    'created_c_pos': self._decode_field(FIELD_LONG)
                }
            )

        items = self._decode_field( FIELD_INT )
        for x in range( 0, items ):

            # (updated-record-count:int)
            # [
            #     (updated-cluster-id:short)
            #     (updated-cluster-position:long)
            #     (new-record-version:int)
            # ]*
            result['updated'].append(
                {
                    'updated_c_id': self._decode_field( FIELD_SHORT ),
                    'updated_c_pos': self._decode_field( FIELD_LONG ),
                    'new_version': self._decode_field( FIELD_INT ),
                }
            )

        items = self._decode_field( FIELD_INT )
        for x in range( 0, items ):

            # (count-of-collection-changes:int)
            # [
            #     (uuid-most-sig-bits:long)
            #     (uuid-least-sig-bits:long)
            #     (updated-file-id:long)
            #     (updated-page-index:long)
            #     (updated-page-offset:int)
            # ]*
            result['updated'].append(
                [
                    self._decode_field(FIELD_LONG),
                    self._decode_field(FIELD_LONG),
                    self._decode_field(FIELD_LONG),
                    self._decode_field(FIELD_LONG),
                    self._decode_field(FIELD_INT),
                ]
            )

        self.dump_streams()

        return result

    def append(self, operation=BaseMessage):

        if isinstance( operation, RecordUpdateMessage ):
            o_record_enc = ORecordEncoder( getattr(operation, "_record_content") )
            self._operation_stack.append( (
                ( FIELD_BYTE, chr(1) ),
                ( FIELD_SHORT, int( getattr(operation, "_cluster_id") ) ),
                ( FIELD_LONG, long( getattr(operation, "_cluster_position") ) ),
                ( FIELD_BYTE, getattr(operation, "_record_type") ),
                ( FIELD_INT, int( getattr(operation, "_record_version") ) ),
                ( FIELD_STRING, o_record_enc.getRaw() ),
                ( FIELD_BOOLEAN, bool( getattr(operation, "_update_content") ) )
            ) )
        elif isinstance( operation, RecordDeleteMessage ):
            self._operation_stack.append( (
                ( FIELD_BYTE, chr(2) ),
                ( FIELD_SHORT, int( getattr(operation, "_cluster_id") ) ),
                ( FIELD_LONG, long( getattr(operation, "_cluster_position") ) ),
                ( FIELD_BYTE, getattr(operation, "_record_type") ),
                ( FIELD_INT, int( getattr(operation, "_record_version") ) ),
            ) )
        elif isinstance( operation, RecordCreateMessage ):
            o_record_enc = ORecordEncoder( getattr(operation, "_record_content") )
            self._operation_stack.append( (
                ( FIELD_BYTE, chr(3) ),
                ( FIELD_SHORT, int( -1 ) ),
                ( FIELD_LONG, long( self._temp_cluster_position_seq ) ),
                ( FIELD_BYTE, getattr(operation, "_record_type") ),
                ( FIELD_STRING, o_record_enc.getRaw() ),
            ) )
            self._temp_cluster_position_seq -= 1
        else:
            raise PyOrientBadMethodCallException(
                "Wrong command type " + operation.__class__.__name__, []
            )

        return self

    def get_transaction_id(self):

        if self.tx_id < 0:
            from datetime import datetime

            my_epoch = datetime(2014, 07, 01)
            now = datetime.now()
            delta = now - my_epoch

            # write in extended mode to make it easy to read
            # seconds * 1000000 to get the equivalent microseconds
            _sm = ( delta.seconds + delta.days * 24 * 3600 ) * 10 ** 6
            _ms = delta.microseconds

            # remove sign
            # treat as unsigned even when the INT is signed
            #  and take 4 Bytes
            # we need only a transaction unique for this session
            # not a real UUID
            if( _sm + _ms ) & 0x80000000:
                self.tx_id = int( ( _sm + _ms - 0x80000000 ) & 0xFFFFFFFF )
            else:
                self.tx_id = int( ( _sm + _ms ) & 0xFFFFFFFF )

        return self.tx_id

    def begin(self):
        self._operation_stack = []
        self.get_transaction_id()
        self._temp_cluster_position_seq = -2
        return self

    def commit(self):
        result = self.prepare().send().fetch_response()
        self._operation_stack = []
        self.tx_id = -1
        self._temp_cluster_position_seq = -2
        return result