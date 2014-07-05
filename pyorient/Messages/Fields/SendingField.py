import struct

from pyorient.Messages.Fields.OrientPrimitives import *


class SendingField(object):

    def __init__(self, field):
        self.content = ''

        # tuple with type
        t, v = field

        if t == INT:
            self.content = struct.pack("!i", v)
        elif t == SHORT:
            self.content = struct.pack("!h", v)
        elif t == LONG:
            self.content = struct.pack("!q", v)
        elif t == BOOLEAN:
            self.content = chr(1) if v else chr(0)
        elif t == BYTE:
            self.content = v
        elif t == BYTES:
            self.content = struct.pack("!i", len(v)) + v
        elif t == STRING:
            self.content = struct.pack("!i", len(v)) + v
        elif t == STRINGS:
            for s in v:
                self.content += struct.pack("!i", len(s)) + s

    def __str__(self):
        return self.content