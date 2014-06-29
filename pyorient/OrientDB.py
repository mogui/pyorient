# import re
# import json
import os
# from OrientTypes import OrientRecordLink, OrientRecord, OrientBinaryObject
# from ORecordCoder import ORecordDecoder, ORecordEncoder
from OrientException import *

from OrientSocket import OrientSocket
from OrientPrimitives import *
from OrientOperations import *


def dlog( msg ):
    # add check for DEBUG key because KeyError Exception is not caught
    # and if no DEBUG key is set, the driver crash with no reason when
    # connection starts
    if 'DEBUG' in os.environ:
        if os.environ['DEBUG']:
            print "[DEBUG]:: %s" % msg


#
# need connection decorator
def need_connected(wrapp):
    def wrap_function(*args, **kwargs):
        if not args[0].is_connected:
            raise PyOrientConnectionException("You must be connected to issue this command", [])
        return wrapp(*args, **kwargs)

    return wrap_function


#
# need db opened decorator
def need_db_opened(wrapp):
    def wrap_function(*args, **kwargs):
        if args[0].opened_db is None:
            raise PyOrientDatabaseException("You must have an opened database to issue this command", [])
        return wrapp(*args, **kwargs)

    return wrap_function


#
# OrientDB
#
class OrientDB(object):

    # init
    def __init__(self, host, port, user=None, pwd=None, autoconnect=True):
        self.conn = None
        self.server = {
            'host': host,
            'port': port
        }
        self.release = None

        self.is_connected = False
        self.opened_db = None

        self.protocol_version = None
        # version 19: 100% compatible 1.6.1-SNAPSHOT
        # version 18: 100% compatible 1.6-SNAPSHOT
        # version 17: 100% compatible. 1.5
        # version 16: 100% compatible. 1.5-SNAPSHOT
        # version 15: 100% compatible. 1.4-SNAPSHOT
        # version 14: 100% compatible. 1.4-SNAPSHOT
        # version 13: 100% compatible. 1.3-SNAPSHOT
        # version 12: 100% compatible. 1.3-SNAPSHOT
        # version 11: 100% compatible. 1.0-SNAPSHOT
        # version 10: 100% compatible. 1.0rc9-SNAPSHOT
        # version 9: 100% compatible. 1.0rc9-SNAPSHOT
        # version 8: 100% compatible. 1.0rc9-SNAPSHOT
        # version 7: 100% compatible. 1.0rc7-SNAPSHOT - 1.0rc8
        # version 6: 100% compatible. Before 1.0rc7-SNAPSHOT
        # < version 6: not compatible

        # If auto-connect is false
        # or we didn't give credential we don't immediately connect
        if autoconnect and user and pwd:
            if self.connect(user, pwd) < 0:
                raise PyOrientConnectionException("Not connected to DB")

    # ------------------------ #
    # REQUESTS IMPLEMENTATIONS #
    # ------------------------ #

    # REQUEST_SHUTDOWN
    @need_connected
    def shutdown(self, user, pwd):
        # todo: make this check a function decorator
        #if not self.is_connected:
        # raise PyOrientConnectionException(
        #   "You must be connected to issue this command", [])

        self.conn.make_request(SHUTDOWN, [user, pwd])

        # we do not have any response for this command :P

    # CONNECT
    def connect(self, user, pwd):

        # int the connection
        self.conn = OrientSocket(self.server['host'], int(self.server['port']))

        # retrieve protocol version
        self.protocol_version = self.conn.read_short()
        # todo: decide whether give up if protocol is not supported

        # packing command
        self.conn.make_request(CONNECT,
                               ["OrientDB Python client (pyorient)",
                                "1.0",
                                (SHORT, 19),
                                "",
                                user,
                                pwd]
                               )

        ok, errors, session_id = self.conn.parse_response([INT])
        if not ok:
            raise PyOrientConnectionException("Error during connection", errors)

        dlog("Session ID: %s" % session_id)
        self.is_connected = True
        self.conn.set_session_id( session_id )
        return session_id

    # DB_OPEN
    def db_open(self, db_name, user, pwd, dbtype=DB_TYPE_DOCUMENT):
        # if not init the connection
        if not self.conn:
            if self.connect(user, pwd) < 0:
                raise PyOrientConnectionException("Not connected to DB")

        self.conn.make_request(DB_OPEN,
                               ["OrientDB Python client (pyorient)",
                                "1.0",
                                (SHORT, 19),
                                "",
                                db_name,
                                dbtype,
                                user,
                                pwd])

        # I don't use the helper method parse_response cause
        # dbopen response is fucken strange !
        status, errors = self.conn.parse_status()
        if not status:
            # null all the expected returns
            raise PyOrientConnectionException( status, errors )

        # Response:
        # (session-id:int)
        # (num-of-clusters:short)
        # [
        #   (cluster-name:string)
        #   (cluster-id:short)
        #   (cluster-type:string)
        #   (cluster-dataSegmentId:short)
        # ]
        # (cluster-config:bytes)
        # (orientdb-release:string)

        # # read session, but we already have one from connect, thrash it
        _session_id = self.conn.read_int()
        # print "%r" % _session_id
        assert _session_id == self.conn.get_session_id()

        self.is_connected = True
        self.opened_db = db_name
        num_ofcluster = self.conn.read_short()
        clusters = []

        for n in range(0, num_ofcluster):
            cluster_name = self.conn.read_string()
            cluster_id = self.conn.read_short()
            cluster_type = self.conn.read_string()
            cluster_segment_data_id = self.conn.read_short()
            clusters.append({
                "name": cluster_name,
                "id": cluster_id,
                "type": cluster_type,
                "segment": cluster_segment_data_id
            })

        cluster_config = self.conn.read_bytes()  # always null
        self.release = self.conn.read_string()

        return clusters

    # REQUEST_DB_CREATE
    @need_connected
    def db_create(self, db_name, dbtype=DB_TYPE_DOCUMENT, storage_type=STORAGE_TYPE_LOCAL):
        self.conn.make_request(DB_CREATE, [db_name, dbtype, storage_type])
        ok, errors = self.conn.parse_response()
        return ok

    # REQUEST_DB_CLOSE
    @need_db_opened
    def db_close(self):
        self.conn.make_request(DB_CLOSE)
        return True

    # REQUEST_DB_EXIST
    @need_connected
    def db_exists(self, *args):  # db_name, server_storage_type
        dbname = args[0]
        server_storage_type = STORAGE_TYPE_LOCAL
        if len(args) > 1:
            server_storage_type = args[1]

        if self.protocol_version < 6:
            self.conn.make_request(DB_EXIST)
        elif self.protocol_version < 16:
            self.conn.make_request(DB_EXIST, [dbname])
        else:  # > 16 1.5-snapshot
            self.conn.make_request(DB_EXIST, [dbname, server_storage_type])

        ok, errors, exists = self.conn.parse_response([BOOLEAN])

        return exists

    # REQUEST_DB_RELOAD
    @need_db_opened
    def db_reload(self):
        self.conn.make_request(DB_RELOAD, [])

        status, errors = self.conn.parse_status()
        if not status:
            # null all the expected returns
            raise PyOrientCommandException( "Reload Failed", errors )

        # Response:
        # (session-id:int)
        # (num-of-clusters:short)
        # [
        #   (cluster-name:string)
        #   (cluster-id:short)
        #   (cluster-type:string)
        #   (cluster-dataSegmentId:short)
        # ]
        # (cluster-config:bytes)
        # (orientdb-release:string)

        num_ofcluster = self.conn.read_short()
        clusters = []

        for n in range(0, num_ofcluster):
            cluster_name = self.conn.read_string()
            cluster_id = self.conn.read_short()
            cluster_type = self.conn.read_string()
            cluster_segment_data_id = self.conn.read_short()
            clusters.append({
                "name": cluster_name,
                "id": cluster_id,
                "type": cluster_type,
                "segment": cluster_segment_data_id
            })
        return clusters

    # REQUEST_DB_DROP
    @need_connected
    def db_drop(self, *args):
        dbname = args[0]
        server_storage_type = STORAGE_TYPE_LOCAL
        if len(args) > 1:
            server_storage_type = args[1]

        if self.protocol_version < 16:
            self.conn.make_request(DB_DROP, [dbname])
        else:  # > 16 1.5-snapshot
            self.conn.make_request(DB_DROP, [dbname, server_storage_type])

        ok, errors = self.conn.parse_response([])
        return ok

    # REQUEST_DB_SIZE
    @need_db_opened
    def db_size(self):
        self.conn.make_request(DB_SIZE)
        ok, errors, size = self.conn.parse_response([LONG])
        return size

    # REQUEST_DB_COUNTRECORDS
    @need_db_opened
    def db_count_records(self):
        self.conn.make_request(DB_COUNTRECORDS)
        ok, errors, count = self.conn.parse_response([LONG])
        return count

    # REQUEST_DATACLUSTER_ADD
    # REQUEST_DATACLUSTER_DROP
    # REQUEST_DATACLUSTER_COUNT
    # REQUEST_DATACLUSTER_DATARANGE
    # REQUEST_DATACLUSTER_COPY
    # REQUEST_DATACLUSTER_LH_CLUSTER_IS_USED
    # REQUEST_DATASEGMENT_ADD
    # REQUEST_DATASEGMENT_DROP
    # REQUEST_RECORD_METADATA
    # REQUEST_RECORD_LOAD
    # REQUEST_RECORD_CREATE
    # REQUEST_RECORD_UPDATE
    # REQUEST_RECORD_DELETE
    # REQUEST_RECORD_COPY
    # REQUEST_POSITIONS_HIGHER
    # REQUEST_POSITIONS_LOWER
    # REQUEST_RECORD_CLEAN_OUT
    # REQUEST_POSITIONS_FLOOR
    # REQUEST_COUNT
    # REQUEST_COMMAND
    # REQUEST_POSITIONS_CEILING
    # REQUEST_TX_COMMIT
    # REQUEST_CONFIG_GET
    # REQUEST_CONFIG_SET
    # REQUEST_CONFIG_LIST
    # REQUEST_DB_RELOAD
    # REQUEST_DB_LISTdb_countrecords
    # REQUEST_PUSH_RECORD
    # REQUEST_PUSH_DISTRIB_CONFIG
    # REQUEST_DB_COPY
    # REQUEST_REPLICATION
    # REQUEST_CLUSTER
    # REQUEST_DB_TRANSFER
    # REQUEST_DB_FREEZE
    # REQUEST_DB_RELEASE
    # REQUEST_DATACLUSTER_FREEZE
    # REQUEST_DATACLUSTER_RELEASE










        # def command(self, query, limit = 20, fetchplan="*:-1", async=False,  **kwargs):
        #     """docstring for command"""


        #     if async:
        #         kwargs['command_type'] = QUERY_ASYNC

        #     raw_result = _pyorient.command(query, limit, **kwargs)

        #     if kwargs.get('raw', False):
        #         return raw_result

        #     ret = []

        #     for raw_record in raw_result:
        #         parser = ORecordDecoder(raw_record)
        #         record = OrientRecord(parser.data, o_class=parser.className)
        #         ret.append(record)

        #     return ret

        # def recordload(self, cluster_id, cluster_position, **kwargs):
        #     raw_record = _pyorient.recordload(cluster_id, cluster_position, **kwargs)
        #     if kwargs.get('raw_result', False):
        #         return raw_record

        #     parser = ORecordDecoder(raw_record)

        #     record = OrientRecord(parser.data, o_class=parser.className, rid="#%d:%d" % (cluster_id, cluster_position))
        #     # @TODO missing rid and version from c api)
        #     return record


        # def recordcreate(self, cluster_id, record, **kwargs):
        #     if not isinstance(record, OrientRecord):
        #         record = OrientRecord(record)

        #     parser = ORecordEncoder(record)
        #     raw_record = parser.getRaw()
        #     ret = _pyorient.recordcreate(cluster_id, raw_record, **kwargs)

        #     return ret

