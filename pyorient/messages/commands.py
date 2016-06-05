# -*- coding: utf-8 -*-
from .base import BaseMessage
from .records import RecordUpdateMessage, RecordDeleteMessage, RecordCreateMessage
from ..exceptions import PyOrientBadMethodCallException
from ..constants import COMMAND_OP, FIELD_BOOLEAN, FIELD_BYTE, FIELD_CHAR, \
    FIELD_INT, FIELD_LONG, FIELD_SHORT, FIELD_STRING, QUERY_SYNC, FIELD_BYTES, \
    TX_COMMIT_OP, QUERY_GREMLIN, QUERY_ASYNC, QUERY_CMD, QUERY_TYPES, \
    QUERY_SCRIPT
from ..utils import need_connected, need_db_opened, dlog


__author__ = 'Ostico <ostico@gmail.com>'


#
# COMMAND_OP
#
# Executes remote commands:
#
# Request: (mode:byte)(class-name:string)(command-payload-length:int)(command-payload)
# Response:
# - synchronous commands: [(synch-result-type:byte)[(synch-result-content:?)]]+
# - asynchronous commands: [(asynch-result-type:byte)[(asynch-result-content:?)]*]
#   (pre-fetched-record-size.md)[(pre-fetched-record)]*+
#
# Where the request:
#
# mode can be 'a' for asynchronous mode and 's' for synchronous mode
# class-name is the class name of the command implementation.
#   There are short form for the most common commands:
# q stands for query as idempotent command. It's like passing
#   com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
# c stands for command as non-idempotent command (insert, update, etc).
#   It's like passing com.orientechnologies.orient.core.sql.OCommandSQL
# s stands for script. It's like passing
#   com.orientechnologies.orient.core.command.script.OCommandScript.
#   Script commands by using any supported server-side scripting like Javascript command. Since v1.0.
# any other values is the class name. The command will be created via
#   reflection using the default constructor and invoking the fromStream() method against it
# command-payload is the command's serialized payload (see Network-Binary-Protocol-Commands)

# Response is different for synchronous and asynchronous request:
# synchronous:
# synch-result-type can be:
# 'n', means null result
# 'r', means single record returned
# 'l', collection of records. The format is:
# an integer to indicate the collection size
# all the records one by one
# 'a', serialized result, a byte[] is sent
# synch-result-content, can only be a record
# pre-fetched-record-size, as the number of pre-fetched records not
#   directly part of the result set but joined to it by fetching
# pre-fetched-record as the pre-fetched record content
# asynchronous:
# asynch-result-type can be:
# 0: no records remain to be fetched
# 1: a record is returned as a resultset
# 2: a record is returned as pre-fetched to be loaded in client's cache only.
#   It's not part of the result set but the client knows that it's available for later access
# asynch-result-content, can only be a record
#
class CommandMessage(BaseMessage):

    def __init__(self, _orient_socket):
        super( CommandMessage, self ).__init__(_orient_socket)

        self._query = ''
        self._limit = 20
        self._fetch_plan = '*:0'
        self._command_type = QUERY_SYNC
        self._mod_byte = 's'

        self._append( ( FIELD_BYTE, COMMAND_OP ) )

    @need_db_opened
    def prepare(self, params=None ):

        if isinstance( params, tuple ) or isinstance( params, list ):
            try:
                self.set_command_type( params[0] )

                self._query = params[1]
                self._limit = params[2]
                self._fetch_plan = params[3]

                # callback function use to operate
                # over the async fetched records
                self.set_callback( params[4] )

            except IndexError:
                # Use default for non existent indexes
                pass

        if self._command_type == QUERY_CMD \
                or self._command_type == QUERY_SYNC \
                or self._command_type == QUERY_SCRIPT \
                or self._command_type == QUERY_GREMLIN:
            self._mod_byte = 's'
        else:
            if self._callback is None:
                raise PyOrientBadMethodCallException( "No callback was provided.", [])
            self._mod_byte = 'a'

        _payload_definition = [
            ( FIELD_STRING, self._command_type ),
            ( FIELD_STRING, self._query )
        ]

        if self._command_type == QUERY_ASYNC \
                or self._command_type == QUERY_SYNC \
                or self._command_type == QUERY_GREMLIN:

            # a limit specified in a sql string should always override a
            # limit parameter pass to prepare()
            if ' LIMIT ' not in self._query.upper() or self._command_type == QUERY_GREMLIN:
                _payload_definition.append( ( FIELD_INT, self._limit ) )
            else:
                _payload_definition.append( ( FIELD_INT, -1 ) )

            _payload_definition.append( ( FIELD_STRING, self._fetch_plan ) )

        if self._command_type == QUERY_SCRIPT:
            _payload_definition.insert( 1, ( FIELD_STRING, 'sql' ) )

        _payload_definition.append( ( FIELD_INT, 0 ) )

        payload = b''.join(
            self._encode_field( x ) for x in _payload_definition
        )

        self._append( ( FIELD_BYTE, self._mod_byte ) )
        self._append( ( FIELD_STRING, payload ) )

        return super( CommandMessage, self ).prepare()

    def fetch_response(self):

        # skip execution in case of transaction
        if self._orientSocket.in_transaction is True:
            return self

        # decode header only
        super( CommandMessage, self ).fetch_response()

        if self._command_type == QUERY_ASYNC:
            self._read_async_records()
        else:
            return self._read_sync()

    def set_command_type(self, _command_type):
        if _command_type in QUERY_TYPES:
            # user choice if present
            self._command_type = _command_type
        else:
            raise PyOrientBadMethodCallException(
                _command_type + ' is not a valid command type', []
            )
        return self

    def set_fetch_plan(self, _fetch_plan):
        self._fetch_plan = _fetch_plan
        return self

    def set_query(self, _query):
        self._query = _query
        return self

    def set_limit(self, _limit):
        self._limit = _limit
        return self

    def _read_sync(self):

        # type of response
        # decode body char with flag continue ( Header already read )
        response_type = self._decode_field( FIELD_CHAR )
        if not isinstance(response_type, str):
            response_type = response_type.decode()
        res = []
        if response_type == 'n':
            self._append( FIELD_CHAR )
            super( CommandMessage, self ).fetch_response(True)
            # end Line \x00
            return None
        elif response_type == 'r' or response_type == 'w':
            res = [ self._read_record() ]
            self._append( FIELD_CHAR )
            # end Line \x00
            _res = super( CommandMessage, self ).fetch_response(True)
            if response_type == 'w':
                res = [ res[0].oRecordData['result'] ]
        elif response_type == 'a':
            self._append( FIELD_STRING )
            self._append( FIELD_CHAR )
            res = [ super( CommandMessage, self ).fetch_response(True)[0] ]
        elif response_type == 'l':
            self._append( FIELD_INT )
            list_len = super( CommandMessage, self ).fetch_response(True)[0]

            for n in range(0, list_len):
                res.append( self._read_record() )

            # async-result-type can be:
            # 0: no records remain to be fetched
            # 1: a record is returned as a result set
            # 2: a record is returned as pre-fetched to be loaded in client's
            #       cache only. It's not part of the result set but the client
            #       knows that it's available for later access
            cached_results = self._read_async_records()
            # cache = cached_results['cached']
        else:
            # this should be never happen, used only to debug the protocol
            msg = b''
            self._orientSocket._socket.setblocking( 0 )
            m = self._orientSocket.read(1)
            while m != "":
                msg += m
                m = self._orientSocket.read(1)

        return res

    def set_callback(self, func):
        if hasattr(func, '__call__'):
            self._callback = func
        else:
            raise PyOrientBadMethodCallException( func + " is not a callable "
                                                         "function", [])
        return self
#
# TX COMMIT
#
# Commits a transaction. This operation flushes all the
#   pending changes to the server side.
#
# Request: (tx-id:int)(using-tx-log:byte)(tx-entry)*(0-byte indicating end-of-records)

#   tx-entry: (operation-type:byte)(cluster-id:short)
#       (cluster-position:long)(record-type:byte)(entry-content)
#
#     entry-content for CREATE: (record-content:bytes)
#     entry-content for UPDATE: (version:record-version)(content-changed:boolean)(record-content:bytes)
#     entry-content for DELETE: (version:record-version)

# Response: (created-record-count:int)[(client-specified-cluster-id:short)
#   (client-specified-cluster-position:long)(created-cluster-id:short)
#   (created-cluster-position:long)]*(updated-record-count:int)[(updated-cluster-id:short)
#   (updated-cluster-position:long)(new-record-version:int)]*(count-of-collection-changes:int)
#   [(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)
#   (updated-page-offset:int)]*
#
# Where:
# tx-id is the Transaction's Id
# use-tx-log tells if the server must use the Transaction
#   Log to recover the transaction. 1 = true, 0 = false
# operation-type can be:
# 1, for UPDATES
# 2, for DELETES
# 3, for CREATIONS
#
# record-content depends on the operation type:
# For UPDATED (1): (original-record-version:int)(record-content:bytes)
# For DELETED (2): (original-record-version:int)
# For CREATED (3): (record-content:bytes)
#
# This response contains two parts: a map of 'temporary' client-generated
#   record ids to 'real' server-provided record ids for each CREATED record,
#   and a map of UPDATED record ids to update record-versions.
#
# Look at Optimistic Transaction to know how temporary RecordIDs are managed.
#
# The last part or response is referred to RidBag management.
#   Take a look at the main page for more details.
class _TXCommitMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(_TXCommitMessage, self).__init__(_orient_socket)

        self._tx_id = -1
        self._operation_stack = []
        self._pre_operation_records = {}
        self._operation_records = {}

        self._temp_cluster_position_seq = -2

        # order matters
        self._append(( FIELD_BYTE, TX_COMMIT_OP ))
        self._command = TX_COMMIT_OP

    @need_connected
    def prepare(self, params=None):

        self._append(( FIELD_INT, self.get_transaction_id() ))
        self._append(( FIELD_BOOLEAN, True ))

        for k, v in enumerate(self._operation_stack):
            self._append(( FIELD_BYTE, chr(1) ))  # start of records
            for field in v:
                self._append(field)

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
            record.update(__version=1, __rid=rid)

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
                    __version=result['updated'][-1]['new_version'],
                    __rid=rid
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
            o_record_enc = self.get_serializer().encode(getattr(operation, "_record_content"))
            self._operation_stack.append((
                ( FIELD_BYTE, chr(1) ),
                ( FIELD_SHORT, int(getattr(operation, "_cluster_id")) ),
                ( FIELD_LONG, int(getattr(operation, "_cluster_position")) ),
                ( FIELD_BYTE, getattr(operation, "_record_type") ),
                ( FIELD_INT, int(getattr(operation, "_record_version")) ),
                ( FIELD_STRING, o_record_enc ),
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
                ( FIELD_LONG, int(getattr(operation, "_cluster_position")) ),
                ( FIELD_BYTE, getattr(operation, "_record_type") ),
                ( FIELD_INT, int(getattr(operation, "_record_version")) ),
            ))
        elif isinstance(operation, RecordCreateMessage):
            o_record_enc = self.get_serializer().encode(getattr(operation, "_record_content"))
            self._operation_stack.append((
                ( FIELD_BYTE, chr(3) ),
                ( FIELD_SHORT, int(-1) ),
                ( FIELD_LONG, int(self._temp_cluster_position_seq) ),
                ( FIELD_BYTE, getattr(operation, "_record_type") ),
                ( FIELD_STRING, o_record_enc ),
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

            my_epoch = datetime(2014, 7, 1)
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
        self._orientSocket.in_transaction = True
        self.get_transaction_id()
        return self

    def commit(self):
        self._orientSocket.in_transaction = False
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
        self._orientSocket.in_transaction = False
        return self

#
# TX COMMIT facade
#
class TxCommitMessage:

    def __init__(self, _orient_socket):
        self._transaction = _TXCommitMessage(_orient_socket)
        pass

    def attach(self, operation):
        self._transaction.attach( operation )
        return self

    def begin(self):
        self._transaction.begin()
        return self

    def commit(self):
        return self._transaction.commit()

    def rollback(self):
        return self._transaction.rollback()

    def set_session_token(self, token):
        self._transaction.set_session_token(token)
        return self
