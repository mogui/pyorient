import re
import json
import os
import struct
from OrientTypes import OrientRecordLink, OrientRecord, OrientBinaryObject
from ORecordCoder import ORecordDecoder, ORecordEncoder
from OrientException import PyOrientConnectionException, PyOrientDatabaseException
import socket

# Operations
SHUTDOWN    = chr(1)
CONNECT     = chr(2)
DB_OPEN     = chr(3)
DB_CREATE   = chr(4)
DB_CLOSE    = chr(5)
DB_EXIST    = chr(6)
DB_RELOAD   = chr(73)
DB_DROP     = chr(7)
DB_SIZE     = chr(8)
DB_COUNTRECORDS = chr(9)



# Types Constants
BOOLEAN = 1  # Single byte: 1 = true, 0 = false
BYTE = 2
SHORT = 3
INT = 4
LONG = 5
BYTES = 6  # Used for binary data.
STRING = 7
RECORD = 8
STRINGS = 9

DB_TYPE_DOCUMENT    = 'document'
DB_TYPE_GRAPH       = 'graph'

STORAGE_TYPE_LOCAL  = 'local'
STORAGE_TYPE_MEMORY = 'memory'

def dlog(msg):
    if os.environ['DEBUG']:
        print "[DEBUG]:: %s" % msg


class OrientSocket(object):
    """docstring for OrientSocket"""

    def __init__(self, host, port):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.s.connect((host, port))
        except socket.error, e:
            raise PyOrientConnectionException("Socket Error: %s" % e)
        self.buffer = ''

    #
    # Read basic types from socket
    #
    def readBool(self):
        return self.readByte() == 1  # 1 = true, 0 = false

    def readByte(self):
        return ord(self.s.recv(1))

    def readShort(self):
        return struct.unpack('!h', self.s.recv(2))[0]

    def readInt(self):
        return struct.unpack('!i', self.s.recv(4))[0]

    def readLong(self):
        return struct.unpack('!q', self.s.recv(8))[0]

    def readBytes(self):
        l = self.readInt()
        if l == -1:
            return None
        return self.s.recv(l)

    def readString(self):
        return self.readBytes()

    def readRecord(self):
        # TODO: implent
        raise Exception("TO implement")
        pass

    def readStrings(self):
        n = self.readInt()
        a = []
        for i in range(0, n):
            a.append(self.readString())
        return a

    #
    # Write basic types on socket
    #
    def putBool(self, b):
        self.buffer += chr(1) if b else chr(0)

    def putByte(self, c):
        self.buffer += c

    def putShort(self, num):
        self.buffer += struct.pack("!h", num)

    def putInt(self, num):
        self.buffer += struct.pack("!i", num)

    def putLong(self, num):
        self.buffer += struct.pack("!q", num)

    def putBytes(self, bytes):
        self.putInt(len(bytes))
        self.buffer += bytes

    def putString(self, string):
        self.putBytes(string)

    def putRecord(self):
        # TODO: implent
        raise Exception("TO implement")

    def putStrings(self, strings):
        for s in strings:
            self.putString(s)

    #
    # Send and flush the buffer
    #
    def send(self):
        self.s.send(self.buffer)
        self.buffer = ''


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

        self.session_id = -1

        # If autoconnect is false
        # or we didn't give credential we don't immediately connect
        if autoconnect and user and pwd:
            self.connect(user, pwd)
            if self.session_id < 0:
                raise PyOrientConnectionException("Not connected to DB")

    #
    # Prepare a command to be sent on the connection
    # take a list of fields, it autoguess by the type for string and int
    # otherwise it expects a tuple with the right type
    #
    def make_request(self, operation, fields=[]):

        dlog("Making request: (%d) %s" % (ord(operation), fields))

        # write operation
        self.conn.putByte(operation)

        # write current session
        self.conn.putInt(self.session_id)

        # iterate commands
        for field in fields:
            if isinstance(field, str):
                self.conn.putString(field)
            elif isinstance(field, int):
                self.conn.putInt(field)
            else:
                # tuple with type
                t, v = field
                if t == SHORT:
                    self.conn.putShort(v)
                elif t == BYTE:
                    self.conn.putByte(v)
        # end for

        # send command
        self.conn.send()

    #
    # Parse status and eventual errors
    #
    def parse_status(self):
        # get status (0=OK, 1=ERROR)
        status = not self.conn.readBool()

        # get session id
        session_id = self.conn.readInt()
        # todo: check that session is the same??

        errors = []

        if not status:
            # Parse the error
            while self.conn.readBool():
                exception_class = self.conn.readString()
                exception_message = self.conn.readString()
                errors.append((exception_class, exception_message))

        return tuple([status, errors])

    #
    # Parse a response from the server
    # giving back the raw content of the response
    #
    def parse_response(self, types=[]):

        status, errors = self.parse_status()
        if not status:
            # null all the expected returns
            content = [None for t in types]
            return tuple([status, errors] + content)

        content = []

        for t in types:
            if t == INT:
                content.append(self.conn.readInt())
            elif t == SHORT:
                content.append(self.conn.readShort())
            elif t == LONG:
                content.append(self.conn.readLong())
            elif t == BOOLEAN:
                content.append(self.conn.readBool())
            elif t == BYTE:
                content.append(self.conn.readByte())
            elif t == BYTES:
                content.append(self.conn.readBytes())
            elif t == STRING:
                content.append(self.conn.readString())
            elif t == STRINGS:
                content.append(self.conn.readStrings())
            elif t == RECORD:
                content.append(self.conn.readRecord())

        return tuple([status, errors] + content)


    # ------------------------ #
    # REQUESTS IMPLEMENTATIONS #
    # ------------------------ #

    # REQUEST_SHUTDOWN
    @need_connected
    def shutdown(self, user, pwd):
        # todo: make this check a function decorator
        #if not self.is_connected:
        #    raise PyOrientConnectionException("You must be connected to issue this command", [])

        self.make_request(SHUTDOWN, [user, pwd])

        # we do not have any respons efor this command :P


    # CONNECT
    def connect(self, user, pwd):

        # int the connection
        self.conn = OrientSocket(self.server['host'], int(self.server['port']))

        # retrieve protocol version
        self.protocol_version = self.conn.readShort()
        # todo: decide whether give up if protocol is not supported

        # packing command
        self.make_request(CONNECT, ["OrientDB Python client (pyorient)", "1.0", (SHORT, 19), "", user, pwd])

        ok, errors, session_id = self.parse_response([INT])
        if not ok:
            raise PyOrientConnectionException("Error during connection", errors)

        dlog("Session ID: %s" % session_id)
        self.is_connected = True
        self.session_id = session_id
        return session_id


    # DB_OPEN
    def db_open(self, dbname, user, pwd, dbtype=DB_TYPE_DOCUMENT):
        # if not init the connection
        if not self.conn:
            self.conn = OrientSocket(self.server['host'], int(self.server['port']))
            # retrieve protocol version
            self.protocol_version = self.conn.readShort()

        self.make_request(DB_OPEN, [
            "OrientDB Python client (pyorient)",
            "1.0",
            (SHORT, 19),
            "",
            dbname,
            dbtype,
            user,
            pwd])

        # I don't use the helper method parse_response cause dbopen response is fucken strange !
        status, errors = self.parse_status()
        if not status:
            # null all the expected returns
            return tuple([status, errors, None, None, None])

        # Response: (session-id:int)(num-of-clusters:short)
        # [(cluster-name:string)(cluster-id:short)(cluster-type:string)
        # (cluster-dataSegmentId:short)] (cluster-config:bytes)(orientdb-release:string)

        # read session
        self.session_id = self.conn.readInt()
        self.is_connected = True
        self.opened_db = dbname
        num_ofcluster = self.conn.readShort()
        clusters = []

        for n in range(0, num_ofcluster):
            cluster_name = self.conn.readString()
            cluster_id = self.conn.readShort()
            cluster_type = self.conn.readString()
            cluster_segmentDataId = self.conn.readShort()
            clusters.append({
                "name": cluster_name,
                "id": cluster_id,
                "type": cluster_type,
                "segment": cluster_segmentDataId
            })

        cluster_config = self.conn.readBytes()  #always null
        self.release = self.conn.readString()

        return clusters

    # REQUEST_DB_CREATE
    @need_connected
    def db_create(self, dbname, dbtype=DB_TYPE_DOCUMENT, storage_type=STORAGE_TYPE_LOCAL):
        self.make_request(DB_CREATE, [dbname, dbtype, storage_type])
        ok, errors = self.parse_response()
        return ok

    # REQUEST_DB_CLOSE
    @need_db_opened
    def db_close(self):
        self.make_request(DB_CLOSE)
        return True

    # REQUEST_DB_EXIST
    @need_connected
    def db_exists(self, *args): # dbname, server_storage_type
        dbname = args[0]
        server_storage_type = STORAGE_TYPE_LOCAL
        if len(args) > 1:
            server_storage_type = args[1]

        if self.protocol_version < 6:
            self.make_request(DB_EXIST)
        elif self.protocol_version < 16:
            self.make_request(DB_EXIST, [dbname])
        else:  # > 16 1.5-snapshot
            self.make_request(DB_EXIST, [dbname, server_storage_type])

        ok, errors, exists = self.parse_response([BOOLEAN])

        return exists

    # REQUEST_DB_RELOAD
    @need_db_opened
    def db_reload(self):
        self.make_request(DB_RELOAD, [])
        # Response:(num-of-clusters:short)[(cluster-name:string)(cluster-id:short)(cluster-type:string)(cluster-dataSegmentId:short)]
        status, errors = self.parse_status()
        if not status:
            # null all the expected returns
            return tuple([status, errors, None, None, None])

        # Response: (session-id:int)(num-of-clusters:short)
        # [(cluster-name:string)(cluster-id:short)(cluster-type:string)
        # (cluster-dataSegmentId:short)] (cluster-config:bytes)(orientdb-release:string)

        num_ofcluster = self.conn.readShort()
        clusters = []

        for n in range(0, num_ofcluster):
            cluster_name = self.conn.readString()
            cluster_id = self.conn.readShort()
            cluster_type = self.conn.readString()
            cluster_segmentDataId = self.conn.readShort()
            clusters.append({
                "name": cluster_name,
                "id": cluster_id,
                "type": cluster_type,
                "segment": cluster_segmentDataId
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
            self.make_request(DB_DROP, [dbname])
        else:  # > 16 1.5-snapshot
            self.make_request(DB_DROP, [dbname, server_storage_type])

        ok, errors = self.parse_response([])
        return ok

    # REQUEST_DB_SIZE
    @need_db_opened
    def db_size(self):
        self.make_request(DB_SIZE)
        ok, errors, size = self.parse_response([LONG])
        return size

    # REQUEST_DB_COUNTRECORDS
    @need_db_opened
    def db_countrecords(self):
        self.make_request(DB_COUNTRECORDS)
        ok, errors, count = self.parse_response([LONG])
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
    # REQUEST_DB_LIST
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

