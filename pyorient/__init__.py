__author__ = 'Ostico <ostico@gmail.com>'

from .orient import OrientDB, OrientSocket
from .exceptions import *
from .OrientTypes import *

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


# Orient User Primitives

DB_TYPE_DOCUMENT    = 'document'
DB_TYPE_GRAPH       = 'graph'
# LOCAL deprecated from version 1.5 and removed in protocol 24
STORAGE_TYPE_LOCAL  = 'local'
STORAGE_TYPE_PLOCAL = 'plocal'
STORAGE_TYPE_MEMORY = 'memory'

QUERY_SYNC    = "com.orientechnologies.orient.core.sql.query.OSQLSynchQuery"
QUERY_ASYNC   = "com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery"
QUERY_CMD     = "com.orientechnologies.orient.core.sql.OCommandSQL"
QUERY_GREMLIN = "com.orientechnologies.orient.graph.gremlin.OCommandGremlin"

SERIALIZATION_DOCUMENT2CSV = "ORecordDocument2csv"
SERIALIZATION_SERIAL_BIN   = "ORecordSerializerBinary"

RECORD_TYPE_BYTES    = 'b'
RECORD_TYPE_DOCUMENT = 'd'
RECORD_TYPE_FLAT     = 'f'

CLUSTER_TYPE_PHYSICAL = 'PHYSICAL'
CLUSTER_TYPE_MEMORY   = 'MEMORY'