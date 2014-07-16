__author__ = 'Ostico <ostico@gmail.com>'


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