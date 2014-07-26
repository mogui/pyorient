__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.BaseMessage import BaseMessage
from pyorient.Messages.Constants.OrientOperations import *
from pyorient.Messages.Constants.BinaryTypes import *
from pyorient.Messages.Database.RecordUpdateMessage import RecordUpdateMessage
from pyorient.Messages.Database.RecordCreateMessage import RecordCreateMessage
from pyorient.Messages.Database.RecordDeleteMessage import RecordDeleteMessage
from pyorient.Commons.utils import *
from pyorient.Commons.ORecordCoder import *


class _TXCommitMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(_TXCommitMessage, self).__init__(_orient_socket)

        self._tx_id = -1
        self._operation_stack = []
        self._pre_operation_records = {}
        self._operation_records = {}

        self._temp_cluster_position_seq = -2

        # order matters
        self._append(( FIELD_BYTE, TX_COMMIT ))
        self._command = TX_COMMIT

    @need_connected
    def prepare(self, params=None):

        self._append(( FIELD_INT, self.get_transaction_id() ))
        self._append(( FIELD_BOOLEAN, True ))

        for k, v in enumerate(self._operation_stack):
            self._append(( FIELD_BYTE, chr(1) ))  # start of records
            map(self._append, v)

        self._append(( FIELD_BYTE, chr(0) ))
        self._append(( FIELD_STRING, "" ))

        return super(_TXCommitMessage, self).prepare()

    def send(self):
        return super(_TXCommitMessage, self).send()

    def fetch_response(self):
        # self.dump_streams()

        super(_TXCommitMessage, self).fetch_response()

        result = {
            'created': [],
            'updated': [],
            'changes': []
        }

        items = self._decode_field(FIELD_INT)
        for x in range(0, items):
            # (created-record-count:int)
            # [
            # (client-specified-cluster-id:short)
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

            operation = self._pre_operation_records[
                str(result['created'][-1]['client_c_pos'])
            ]

            rid = "#" + str(result['created'][-1]['created_c_id']) + \
                  ":" + str(result['created'][-1]['created_c_pos'])

            record = getattr(operation, "_record_content")
            record.update(version=1, rid=rid)

            self._operation_records[rid] = record

        items = self._decode_field(FIELD_INT)
        for x in range(0, items):

            # (updated-record-count:int)
            # [
            # (updated-cluster-id:short)
            #     (updated-cluster-position:long)
            #     (new-record-version:int)
            # ]*
            result['updated'].append(
                {
                    'updated_c_id': self._decode_field(FIELD_SHORT),
                    'updated_c_pos': self._decode_field(FIELD_LONG),
                    'new_version': self._decode_field(FIELD_INT),
                }
            )

            try:
                operation = self._pre_operation_records[
                    str(result['updated'][-1]['updated_c_pos'])
                ]
                record = getattr(operation, "_record_content")
                rid = "#" + str(result['updated'][-1]['updated_c_id']) + \
                      ":" + str(result['updated'][-1]['updated_c_pos'])
                record.update(
                    version=result['updated'][-1]['new_version'],
                    rid=rid
                )

                self._operation_records[rid] = record

            except KeyError:
                pass

        if self.get_protocol() > 23:
            items = self._decode_field(FIELD_INT)
            for x in range(0, items):
                # (count-of-collection-changes:int)
                # [
                # (uuid-most-sig-bits:long)
                #     (uuid-least-sig-bits:long)
                #     (updated-file-id:long)
                #     (updated-page-index:long)
                #     (updated-page-offset:int)
                # ]*
                result['updated'].append(
                    {
                        'uuid_high': self._decode_field(FIELD_LONG),
                        'uuid_low': self._decode_field(FIELD_LONG),
                        'file_id': self._decode_field(FIELD_LONG),
                        'page_index': self._decode_field(FIELD_LONG),
                        'page_offset': self._decode_field(FIELD_INT),
                    }
                )

        self.dump_streams()

        return self._operation_records #  [self._operation_records, result]

    def attach(self, operation):

        if not isinstance(operation, BaseMessage):
            # A Subclass of BaseMessage was expected
            raise AssertionError("A subclass of BaseMessage was expected")

        if isinstance(operation, RecordUpdateMessage):
            o_record_enc = ORecordEncoder(getattr(operation, "_record_content"))
            self._operation_stack.append((
                ( FIELD_BYTE, chr(1) ),
                ( FIELD_SHORT, int(getattr(operation, "_cluster_id")) ),
                ( FIELD_LONG, long(getattr(operation, "_cluster_position")) ),
                ( FIELD_BYTE, getattr(operation, "_record_type") ),
                ( FIELD_INT, int(getattr(operation, "_record_version")) ),
                ( FIELD_STRING, o_record_enc.getRaw() ),
            ))

            if self.get_protocol() >= 23:
                self._operation_stack[-1] = \
                    self._operation_stack[-1] +\
                    ( ( FIELD_BOOLEAN, bool(getattr(operation, "_update_content") ) ), )

            self._pre_operation_records[
                str(getattr(operation, "_cluster_position"))
            ] = operation

        elif isinstance(operation, RecordDeleteMessage):
            self._operation_stack.append((
                ( FIELD_BYTE, chr(2) ),
                ( FIELD_SHORT, int(getattr(operation, "_cluster_id")) ),
                ( FIELD_LONG, long(getattr(operation, "_cluster_position")) ),
                ( FIELD_BYTE, getattr(operation, "_record_type") ),
                ( FIELD_INT, int(getattr(operation, "_record_version")) ),
            ))
        elif isinstance(operation, RecordCreateMessage):
            o_record_enc = ORecordEncoder(getattr(operation, "_record_content"))
            self._operation_stack.append((
                ( FIELD_BYTE, chr(3) ),
                ( FIELD_SHORT, int(-1) ),
                ( FIELD_LONG, long(self._temp_cluster_position_seq) ),
                ( FIELD_BYTE, getattr(operation, "_record_type") ),
                ( FIELD_STRING, o_record_enc.getRaw() ),
            ))
            self._pre_operation_records[
                str(self._temp_cluster_position_seq)
            ] = operation
            self._temp_cluster_position_seq -= 1
        else:
            raise PyOrientBadMethodCallException(
                "Wrong command type " + operation.__class__.__name__, []
            )

        return self

    def get_transaction_id(self):

        if self._tx_id < 0:
            from datetime import datetime

            my_epoch = datetime(2014, 07, 01)
            now = datetime.now()
            delta = now - my_epoch

            # write in extended mode to make it easy to read
            # seconds * 1000000 to get the equivalent microseconds
            _sm = ( delta.seconds + delta.days * 24 * 3600 ) * 10 ** 6
            _ms = delta.microseconds
            _mstime = _sm + _ms
            # remove sign
            # treat as unsigned even when the INT is signed
            # and take 4 Bytes
            #   ( 32 bit uniqueness is not ensured in any way,
            #     but is surely unique in this session )
            # we need only a transaction unique for this session
            # not a real UUID
            if _mstime & 0x80000000:
                self._tx_id = int(( _mstime - 0x80000000 ) & 0xFFFFFFFF)
            else:
                self._tx_id = int(_mstime & 0xFFFFFFFF)

        return self._tx_id

    def begin(self):
        self._operation_stack = []
        self._pre_operation_records = {}
        self._operation_records = {}
        self._temp_cluster_position_seq = -2
        self.get_transaction_id()
        return self

    def commit(self):
        result = self.prepare().send().fetch_response()
        self._operation_stack = []
        self._pre_operation_records = {}
        self._operation_records = {}
        self._tx_id = -1
        self._temp_cluster_position_seq = -2
        return result

    def rollback(self):
        self._operation_stack = []
        self._pre_operation_records = {}
        self._operation_records = {}
        self._tx_id = -1
        self._temp_cluster_position_seq = -2
        return self
