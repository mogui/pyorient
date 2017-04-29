__author__ = 'mogui <mogui83@gmail.com>'

#
# Driver Constants
#
NAME = "OrientDB Python binary client (pyorient)"
VERSION = "1.5.5"
SUPPORTED_PROTOCOL = 36

#
# Binary Types
#
# Types Constants
BOOLEAN = 1  # Single byte: 1 = true, 0 = false
BYTE    = 2
SHORT   = 3
INT     = 4
LONG    = 5
BYTES   = 6  # Used for binary data.
STRING  = 7
RECORD  = 8
STRINGS = 9
CHAR    = 10
LINK    = 11


# Field types, needed for decoding
# we have the type definition and the number of first bytes to read
FIELD_BOOLEAN = {"type": BOOLEAN, "bytes": 1, "struct": None}
FIELD_BYTE = {"type": BYTE, "bytes": 1, "struct": None}
FIELD_CHAR = {"type": CHAR, "bytes": 1, "struct": None}
FIELD_SHORT = {"type": SHORT, "bytes": 2, "struct": None}
FIELD_INT = {"type": INT, "bytes": 4, "struct": None}
FIELD_LONG = {"type": LONG, "bytes": 8, "struct": None}
FIELD_BYTES = {"type": BYTES, "bytes": 4, "struct": None}
FIELD_STRING = {"type": STRING, "bytes": 4, "struct": None}
FIELD_STRINGS = {"type": STRINGS, "bytes": 4, "struct": None}
FIELD_RECORD = {"type": RECORD, "bytes": None, "struct": [
    FIELD_CHAR,   # record_type
    FIELD_SHORT,  # record_clusterID
    FIELD_LONG,   # record_position
    FIELD_INT,    # record_version
    FIELD_BYTES   # record_content
]}
FIELD_TYPE_LINK = {"type": LINK, "bytes": None, "struct": [
    FIELD_SHORT,  # record_clusterID
    FIELD_LONG,   # record_position
]}


SHUTDOWN                 = "ShutdownMessage"
CONNECT                  = "ConnectMessage"
DB_OPEN                  = "DbOpenMessage"
DB_CREATE                = "DbCreateMessage"
DB_CLOSE                 = "DbCloseMessage"
DB_EXIST                 = "DbExistsMessage"
DB_DROP                  = "DbDropMessage"
DB_SIZE                  = "DbSizeMessage"
DB_COUNT_RECORDS         = "DbCountRecordsMessage"
DATA_CLUSTER_ADD         = "DataClusterAddMessage"
DATA_CLUSTER_DROP        = "DataClusterDropMessage"
DATA_CLUSTER_COUNT       = "DataClusterCountMessage"
DATA_CLUSTER_DATA_RANGE  = "DataClusterDataRangeMessage"
RECORD_LOAD              = "RecordLoadMessage"
RECORD_CREATE            = "RecordCreateMessage"
RECORD_UPDATE            = "RecordUpdateMessage"
RECORD_DELETE            = "RecordDeleteMessage"
COMMAND                  = "CommandMessage"
DB_RELOAD                = "DbReloadMessage"
TX_COMMIT                = "TxCommitMessage"

#
# Orient Operations
#
SHUTDOWN_OP                = chr(1)
CONNECT_OP                 = chr(2)
DB_OPEN_OP                 = chr(3)
DB_CREATE_OP               = chr(4)
DB_CLOSE_OP                = chr(5)
DB_EXIST_OP                = chr(6)
DB_DROP_OP                 = chr(7)
DB_SIZE_OP                 = chr(8)
DB_COUNT_RECORDS_OP        = chr(9)
DATA_CLUSTER_ADD_OP        = chr(10)
DATA_CLUSTER_DROP_OP       = chr(11)
DATA_CLUSTER_COUNT_OP      = chr(12)
DATA_CLUSTER_DATA_RANGE_OP = chr(13)

RECORD_LOAD_OP             = chr(30)
RECORD_CREATE_OP           = chr(31)
RECORD_UPDATE_OP           = chr(32)
RECORD_DELETE_OP           = chr(33)

COMMAND_OP                 = chr(41)

TX_COMMIT_OP               = chr(60)

DB_RELOAD_OP               = chr(73)
DB_LIST_OP                 = chr(74)

#
# Orient Primitives
#

#: Document type
DB_TYPE_DOCUMENT    = 'document'
#: Graph type
DB_TYPE_GRAPH       = 'graph'
DB_TYPES            = (
    DB_TYPE_DOCUMENT,
    DB_TYPE_GRAPH,
)

# LOCAL deprecated from version 1.5 and removed in protocol 24
STORAGE_TYPE_LOCAL  = 'local'
STORAGE_TYPE_PLOCAL = 'plocal'
STORAGE_TYPE_MEMORY = 'memory'
STORAGE_TYPES       = (
    STORAGE_TYPE_LOCAL,
    STORAGE_TYPE_PLOCAL,
    STORAGE_TYPE_MEMORY,
)

QUERY_SYNC    = "com.orientechnologies.orient.core.sql.query.OSQLSynchQuery"
QUERY_ASYNC   = "com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery"
QUERY_CMD     = "com.orientechnologies.orient.core.sql.OCommandSQL"
QUERY_GREMLIN = "com.orientechnologies.orient.graph.gremlin.OCommandGremlin"
QUERY_SCRIPT  = "com.orientechnologies.orient.core.command.script.OCommandScript"
QUERY_TYPES   = (
    QUERY_SYNC,
    QUERY_ASYNC,
    QUERY_CMD,
    QUERY_GREMLIN,
    QUERY_SCRIPT,
)

RECORD_TYPE_BYTES    = 'b'
RECORD_TYPE_DOCUMENT = 'd'
RECORD_TYPE_FLAT     = 'f'
RECORD_TYPES         = (
    RECORD_TYPE_BYTES,
    RECORD_TYPE_DOCUMENT,
    RECORD_TYPE_FLAT,
)

CLUSTER_TYPE_PHYSICAL = 'PHYSICAL'
CLUSTER_TYPE_MEMORY   = 'MEMORY'
CLUSTER_TYPES         = (
    CLUSTER_TYPE_PHYSICAL,
    CLUSTER_TYPE_MEMORY
)


# OTHER CONFIGURATIONS
SOCK_CONN_TIMEOUT = 30
