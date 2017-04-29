import re
import sys
import time
from datetime import date, datetime
from decimal import Decimal, localcontext
from .otypes import OrientRecordLink, OrientRecord, OrientBinaryObject
from .exceptions import PyOrientBadMethodCallException
try:
    import pyorient_native
    binary_support = True
except:
    binary_support = False


class OrientSerializationBinary(object):
    def __init__(self, props):
        self.className = None
        self.data = {}
        self.type = OrientSerialization.Binary
        self.props = props
        self._writer = None
        
    def decode(self, content):
        clsname, data = pyorient_native.deserialize(content,
                                    content.__sizeof__(), self.props)
        rels = [k for k in data.keys() if ('in_' in k or 'out_' in k
                                       or k=='in' or k=='out')] 
        for k in rels:
            if isinstance(data[k],list):
                for i in range(len(data[k])):
                    data[k][i] = OrientRecordLink(str(data[k][i][1]) + ':' +
                                                  str(data[k][i][2]))
            elif isinstance(data[k],tuple):
                data[k] = OrientRecordLink(str(data[k][1]) + ':' +
                                                  str(data[k][2]))
        return [clsname, data]

    def encode(self, record):
        if record:
            return pyorient_native.serialize(record)
        else:
            return None


###########################################################
# Regular expressions to speed up OrientSerializationCSV  #
###########################################################

# number, possibly floating point, possibly floating with exponent:
numRegex = re.compile('-?[0-9]+(\.[0-9]+)?(E-?[0-9]+)?')
# -?             optional minus sign
# [0-9]+         one or more digits
# (\.[0-9]+)?    optional decimal and digits
#     \.             decimal point
#     [0-9]+         one or more digits
# (E-?[0-9]+)?   optional exponent
#     E              E
#     -?             optional minus sign
#     [0-9]+         one or more digits

# RID in the form of number:number
ridRegex = re.compile('-?[0-9]+:[0-9]+')
# -?                optional minus sign
# [0-9]+            one or more digits
# :                 colon
# [0-9]+            one or more digits

# characters leading up to (but not including) the first quote that isn't escaped:
strRegex = re.compile(r'(?s)((\\\\)*(?="))|(.*?[^\\](\\\\)*(?="))') 
# (?s)                      single line mode, dot matches everything including newlines
# ((\\\\)*(?="))            even number of backslashes followed by quote
#    (\\\\)*                two backslashes, zero or more times
#    (?=")                  lookahead to see next character is a quote, but don't include in match
# |                         or
# (.*?[^\\](\\\\)*(?="))    any number of characters, even num backslash, followed by a quote
#    .*?                    any character, any number of times, smallest match instead of greedy
#    [^\\]                  any character that is not a backslash
#    (\\\\)*                two backslashes, zero or more times
#    (?=")                  lookahead to see next character is a quote, but don't include in match

# escaped character
escRegex = re.compile(r'(?s)\\.')
# (?s)                      single line mode, dot matches everything including newlines
# \\.                       single backslash followed by any character

# bag data
bagRegex = re.compile('[^;]*')
# [^                        exclude the following characters
#   ;                       exclude semicolon
# ]*                        match any number of characters that are not excluded

# binary data
binRegex = re.compile('[^_,)>\}\]]*')
# [^                        exclude the following characters
#   _,)>\}\]                characters to exclude (backslash escapes curly brace and square brace)
# ]*                        match any number of characters that are not excluded


class OrientSerializationCSV(object):

    def __init__(self):
        self.className = None
        self.data = {}
        self.type = OrientSerialization.CSV

    def decode(self, content):
        """
         Deserialize a record.
           :param content str The input to un-serialize.
           :return: (class_name, dict)
        """

        if not content:
            return self.className, self.data

        if not isinstance(content, str):
            content = content.decode()

        content = content.strip()

        collected, offset, is_class_name = self._parse_first_key(content, offset=0)

        if is_class_name:
            # this is actually a class name.
            self.className = collected
            key, offset = self._parse_key(content, offset)
        else:
            key = collected

        if not key and not (offset < len(content)):
            return self.className, self.data

        value, offset = self._parse_value(content, offset)
        
        self.data[key] = value

        while offset < len(content):
            if content[offset] == ',':
                offset += 1
            else:
                break

            key, offset = self._parse_key(content, offset)
            if offset < len(content):
                value, offset = self._parse_value(content, offset)
                self.data[key] = value
            else:
                self.data[key] = None

        return self.className, self.data

    def encode(self, record):
        """
        Encode an OrientRecord to be sent over the connection

        :param record: :class: `OrientRecord <pyorient.types.OrientRecord>`
        :return: raw string to send over the wire
        """
        raw = ''
        o_class = getattr(record, '_class', False)
        if o_class:
            raw = o_class + '@'

        fields = list(record.oRecordData)

        for idx, key in enumerate(fields):
            raw += key + ':'
            value = record.oRecordData[key]
            raw += self._encode_value(value)

            if idx < len(list(fields)) - 1:
                # not last element
                raw += ','

        return raw

    #
    # ENCODING STUFF
    #
    def _encode_value(self, value):

        if isinstance(value, str):
            ret = '"' + value + '"'
        elif isinstance(value, float):
            with localcontext() as ctx:
                ctx.prec = 20  # floats are max 80-bits wide = 20 significant digits
                ret = '{:f}d'.format(Decimal(value))
        elif sys.version_info[0] >= 3 and isinstance(value, int):
            if value > 2147483647:
                ret = str(value) + 'l'
            else:
                ret = str(value)
        elif sys.version_info[0] < 3 and isinstance(value, long):
            ret = str(value) + 'l'
        elif isinstance(value, int):
            ret = str(value)
        elif isinstance(value, datetime):
            ret = str(int(time.mktime(value.timetuple())) * 1000) + 't'
        elif isinstance(value, date):
            ret = str(int(time.mktime(value.timetuple())) * 1000) + 'a'
        elif isinstance(value, Decimal):
            ret = '{:f}c'.format(value)
        elif isinstance(value, list):
            try:
                base_cls = type(value[0])
            except IndexError:
                elements = value
            else:
                if issubclass(base_cls, OrientRecordLink):
                    elements = [elem.get_hash() for elem in value]
                else:
                    elements = [self._encode_value(elem) for elem in value]

            ret = "[" + ",".join(elements) + "]"
        elif isinstance(value, dict):
            ret = "{" + ','.join(map(lambda elem: '"' + elem + '":' + self._encode_value(value[elem]), value)) + '}'
        elif isinstance(value, OrientRecord):
            ret = "(" + self.encode(value.oRecordData) + ")"
        elif isinstance(value, OrientRecordLink):
            ret = value.get_hash()
        elif isinstance(value, OrientBinaryObject):
            ret = value.get_hash()
        else:
            ret = ''
        return ret

    #
    # DECODING STUFF
    #

    def _parse_first_key(self, content, offset):
        """
          Consume the first field key, which could be a class name.
            :param content str The input to consume
            :param offset int Offset into content indicating where to parse
            :return: The collected string, updated offset for subsequent parsing, 
             and a boolean indicating whether this is a class name.
        """

        length = len(content) - offset
        is_class_name = False
        if content[offset] == '"':
            collected, offset = self._parse_string(content, offset+1)
            return collected, offset+1, is_class_name

        i = 0
        while i < length:
            if content[offset+i] == '@':
                is_class_name = True
                break
            elif content[offset+i] == ':':
                break
            i += 1

        return content[offset:offset+i], offset+i+1, is_class_name

    def _parse_key(self, content, offset):
        """
          Consume a field key, which may or may not be quoted.
            :param content str The input to consume
            :param offset int Offset into content indicating where to parse
            :return: The collected string and updated offset for subsequent parsing
        """
        if offset >= len(content):
            return None, offset
        if content[offset] == '"':
            collected, offset = self._parse_string(content, offset+1)
            return collected, offset+1

        collected = content[offset:content.find(':', offset)]

        return collected, offset+len(collected)+1

    def _parse_value(self, content, offset):
        """
          Consume a field value.
            :param: content str The input to consume
            :param offset int Offset into content indicating where to parse
            :return: The collected value and updated offset for subsequent parsing
        """
        c = ''
        
        while offset < len(content) and content[offset] == ' ': offset += 1  # skip leading space
        try:
            c = content[offset]  # string index out of range 0
        except IndexError:
            pass

        if offset >= len(content) or c == ',':
            return None, offset
        elif c in '-0123456789':
            return self._parse_number(content, offset)
        elif c == '#':
            return self._parse_rid(content, offset+1)
        elif c == '"':
            return self._parse_string(content, offset+1)
        elif c == '[':
            return self._parse_collection(content, offset+1)
        elif c == '<':
            return self._parse_set(content, offset+1)
        elif c == '{':
            return self._parse_map(content, offset+1)
        elif c == '(':
            return self._parse_record(content, offset+1)
        elif c == '%':
            return self._parse_bag(content, offset+1)
        elif c == '_':
            return self._parse_binary(content, offset+1)
        elif c == 'n' and content[offset:offset+4] == 'null':
            return None, offset+4
        elif c == 't' and content[offset:offset+4] == 'true':
            return True, offset+4
        elif c == 'f' and content[offset:offset+5] == 'false':
            return False, offset+5
        else:
            return None, offset

    @staticmethod
    def _parse_string(content, offset):
        """
          Consume a string.
            :param content str The input to consume
            :param offset int Offset into content indicating where to parse
            :return: The collected string and updated offset for subsequent parsing
        """
        collected = ''
        match = strRegex.match(content, offset)
        if match:
            collected = match.group(0)
            offset += len(collected)+1
            # replace escape+character with just the character:
            collected = escRegex.sub(lambda m: m.group(0)[-1:], collected)
        return collected, offset

    @staticmethod
    def _parse_number(content, offset):
        """
          Consume a number.
            If the number has a suffix, consume it also and instantiate the
             right type, e.g. for dates
            :param content str The content to consume
            :param offset int Offset into content indicating where to parse
            :return: The collected number and updated offset for subsequent parsing
        """
        collected = ''
        match = numRegex.match(content, offset)
        if match:
            collected = match.group(0)
            offset += len(collected)
        is_float = '.' in collected
        
        c = ''
        try:
            c = content[offset]  # string index out of range 0
        except IndexError:
            pass

        if c == 'c':
            collected = Decimal(collected)
            offset += 1
        elif c == 'd' or c == 'f':
            # double or float
            collected = float(collected)
            offset += 1
        elif c == 'a':
            collected = date.fromtimestamp(float(collected) / 1000)
            offset += 1
        elif c == 't':
            # date
            collected = datetime.fromtimestamp(float(collected) / 1000)
            offset += 1
        elif c == 'b' or c == 's':
            collected = int(collected)
            offset += 1
        elif c == 'l':
            if sys.version_info[0] < 3:
                collected = long(collected)  # python 2.x long type
            else:
                collected = int(collected)
            offset += 1
        elif is_float:
            collected = float(collected)
        else:
            collected = int(collected)

        return collected, offset

    @staticmethod
    def _parse_rid(content, offset):
        """
          Consume a Record ID.
            :param content str The input to consume
            :param offset int Offset into content indicating where to parse
            :return: The collected RID and updated offset for subsequent parsing
        """
        collected = ''
        match = ridRegex.match(content, offset)
        if match:
            collected = match.group(0)
            offset += len(collected)
        return OrientRecordLink(collected), offset

    def _parse_collection(self, content, offset):
        """
            Consume an array of values.
            :param content str The input to consume
            :param offset int Offset into content indicating where to parse
            :return: The collected array and updated offset for subsequent parsing
        """
        collection = []
        while offset < len(content):
            c = content[offset]
            if c == ',':
                offset += 1
            elif c == ']':
                offset += 1
                break

            collected, offset = self._parse_value(content, offset)
            collection.append(collected)

        return collection, offset

    def _parse_set(self, content, offset):
        """
          Consume a set of values.
            :param content str The input to consume
            :param offset int Offset into content indicating where to parse
            :return: The collected set (as a list) and updated offset for subsequent parsing
        """
        list_set = []
        while offset < len(content):
            c = content[offset]
            if c == ',':
                offset += 1
            elif c == '>':
                offset += 1
                break

            collected, offset = self._parse_value(content, offset)
            list_set.append(collected)

        return list_set, offset
        
    def _parse_map(self, content, offset):
        """
          Consume a map of keys to values.
            :param content str The input to consume
            :param offset int Offset into content indicating where to parse
            :return: The collected map and updated offset for subsequent parsing
        """
        _map = {}
        
        while offset < len(content) and content[offset] == ' ': offset += 1  # skip leading space
        
        while offset < len(content):
            c = content[offset]
            
            if c == ',':
                offset += 1
                while offset < len(content) and content[offset] == ' ': offset += 1  # skip leading space
            elif c == '}':
                offset += 1 
                break
            elif c == ' ':
                offset += 1
                while offset < len(content) and content[offset] == ' ': offset += 1  # skip leading space
                continue
                
            key, offset = self._parse_key(content, offset)
            while offset < len(content) and content[offset] == ' ': offset += 1  # skip leading space
            if offset < len(content):
                _map[key], offset = self._parse_value(content, offset)
                while offset < len(content) and content[offset] == ' ': offset += 1  # skip leading space
            else:
                _map[key] = None
                break

        return _map, offset

    def _parse_record(self, content, offset):
        """
          Consume an embedded record.
            :param content str The content to unserialize.
            :param offset int Offset into content indicating where to parse
            :return: The collected record and updated offset for subsequent parsing
        """
        record = {}

        while offset < len(content) and content[offset] == ' ': offset += 1  # skip leading space
        if content[offset] == ')':
            # this is an empty record.
            return record, offset+1

        collected, offset, is_class_name = self._parse_first_key(content, offset)
        if is_class_name:
            # this is actually a class name.
            record['o_class'] = collected
            while offset < len(content) and content[offset] == ' ': offset += 1  # skip leading space
            if content[offset] == ')':
                return record, offset+1

            key, offset = self._parse_key(content, offset)
        else:
            key = collected

        value, offset = self._parse_value(content, offset)
        while offset < len(content) and content[offset] == ' ': offset += 1  # skip leading space

        record[key] = value

        while offset <= len(content):
            if content[offset] == ',':
                offset += 1
                while offset < len(content) and content[offset] == ' ': offset += 1  # skip leading space
            elif content[offset] == ')':
                offset += 1
                while offset < len(content) and content[offset] == ' ': offset += 1  # skip leading space
                break

            key, offset = self._parse_key(content, offset)
            while offset < len(content) and content[offset] == ' ': offset += 1  # skip leading space
            if offset < len(content):
                value, offset = self._parse_value(content, offset)
                record[key] = value
            else:
                record[key] = None

        return record, offset

    @staticmethod
    def _parse_bag(content, offset):
        """
          Consume a record id bag.
            :param content str The content to consume
            :param offset int Offset into content indicating where to parse
            :return: The collected record id bag and updated offset for subsequent parsing
        """

        collected = bagRegex.match(content, offset).group(0)
        return OrientBinaryObject(collected), offset + len(collected) + 1
        
    @staticmethod
    def _parse_binary(content, offset):
        """
          Consume a binary field.
            :param content str The content to consume
            :param offset int Offset into content indicating where to parse
            :return: The collected binary and updated offset for subsequent parsing
        """

        collected = binRegex.match(content, offset).group(0)
        return collected, offset + len(collected) + 1


class OrientSerialization(object):
    """
    Enum representing the available serialization
    """
    #: CSV the default serialization
    CSV = "ORecordDocument2csv"

    #: Now unimplemented
    Binary = "ORecordSerializerBinary"

    @classmethod
    def get_impl(cls, impl, props=None):
        impl_map = {
            cls.CSV: OrientSerializationCSV,
            cls.Binary: OrientSerializationBinary,
        }
        implementation = impl_map.get(impl, False)
        if not implementation:
            raise PyOrientBadMethodCallException(
                impl + ' is not an available serialization type', []
            )
        if impl == cls.Binary:
            if not binary_support:
                raise Exception( "To support Binary Serialization, pyorient_native must be installed" )
            return implementation(props)
        else:
            return implementation()
