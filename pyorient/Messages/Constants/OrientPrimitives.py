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


# Field types, we have the type definition and the first bytes to read
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
    FIELD_SHORT,
    FIELD_BYTE,
    FIELD_SHORT,
    FIELD_LONG,
    FIELD_INT,
    FIELD_BYTES
]}


DB_TYPE_DOCUMENT    = 'document'
DB_TYPE_GRAPH       = 'graph'

STORAGE_TYPE_LOCAL = 'local'
STORAGE_TYPE_PLOCAL = 'plocal'
STORAGE_TYPE_MEMORY = 'memory'

# Commands costants
QUERY_SYNC  = "com.orientechnologies.orient.core.sql.query.OSQLSynchQuery"
QUERY_ASYNC = "com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery"
QUERY_CMD   = "com.orientechnologies.orient.core.sql.OCommandSQL"

SERIALIZATION_DOCUMENT2CSV = "ORecordDocument2csv"
SERIALIZATION_SERIAL_BIN = "ORecordSerializerBinary"