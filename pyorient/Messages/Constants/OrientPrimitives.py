__author__ = 'Ostico <ostico@gmail.com>'

DB_TYPE_DOCUMENT    = 'document'
DB_TYPE_GRAPH       = 'graph'

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