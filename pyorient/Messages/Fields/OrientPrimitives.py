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


DB_TYPE_DOCUMENT    = 'document'
DB_TYPE_GRAPH       = 'graph'

STORAGE_TYPE_LOCAL = 'local'
STORAGE_TYPE_PLOCAL = 'plocal'
STORAGE_TYPE_MEMORY = 'memory'

# Commands costants
QUERY_SYNC  = "com.orientechnologies.orient.core.sql.query.OSQLSynchQuery"
QUERY_ASYNC = "com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery"
QUERY_CMD   = "com.orientechnologies.orient.core.sql.OCommandSQL"