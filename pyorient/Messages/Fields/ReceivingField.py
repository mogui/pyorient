__author__ = 'Ostico'

import struct
from pyorient.Messages.Fields.OrientPrimitives import *


class ReceivingField(object):

    def __init__(self, _type):
        self.content = 0
        self.type = 0

        if _type == INT:
            self.type = INT
            self.content = 4
        elif _type == SHORT:
            self.type = SHORT
            self.content = 2
        elif _type == LONG:
            self.type = LONG
            self.content = 8
        elif _type == BOOLEAN:
            self.type = BOOLEAN
            self.content = 1
        elif _type == BYTE:
            self.type = BYTE
            self.content = 1
        elif _type == BYTES:
            self.type = BYTES
            self.content = 4
        elif _type == STRING:
            self.type = STRING
            self.content = 4

    @staticmethod
    def decode(_type, _value):
        if _type == BOOLEAN:
            return ord(_value) == 1
        elif _type == BYTE:
            return ord(_value)
        elif _type == SHORT:
            return struct.unpack('!h', _value)[0]
        elif _type == INT:
            return struct.unpack('!i', _value)[0]
        elif _type == LONG:
            return struct.unpack('!q', _value)[0]
        elif _type == STRING:
            # _len = ReceivingField.decode( INT, _value[0] )
            # return _value[1:]
            return ''
        elif _type == BYTES:
            # _len = ReceivingField.decode( INT, _value[0] )
            # return _value[1:]
            return ''