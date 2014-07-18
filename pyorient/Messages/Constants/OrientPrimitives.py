__author__ = 'Ostico <ostico@gmail.com>'

DB_TYPE_DOCUMENT    = 'document'
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
QUERY_TYPES   = (
    QUERY_SYNC,
    QUERY_ASYNC,
    QUERY_CMD,
    QUERY_GREMLIN,
)

SERIALIZATION_DOCUMENT2CSV = "ORecordDocument2csv"
SERIALIZATION_SERIAL_BIN   = "ORecordSerializerBinary"
SERIALIZATION_TYPES        = (
    SERIALIZATION_DOCUMENT2CSV,
    SERIALIZATION_SERIAL_BIN,
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