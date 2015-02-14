__author__ = 'gremorian'

import sys
import time
from datetime import date, datetime
from .types import OrientRecordLink, OrientRecord, OrientBinaryObject


class ORecordDecoder(object):

    def __init__(self, content):
        self.className = None
        self.data = {}

        if not isinstance(content, str):
            content = content.decode()
        self.__decode( content )

    def __decode( self, content ):
        """
         Deserialize a record.
           :param content str The input to un-serialize.
           :return: dict|None The un-serialized document, or None if the input is empty.
        """
        if not content:
            return

        content = content.strip()

        chunk = self.parse_first_key( content )

        if chunk[2]:
            # this is actually a class name.
            self.className = chunk[0]
            content = chunk[1]
            chunk = self.parse_key(content)
            key = chunk[0]
            content = chunk[1]
        else:
            key = chunk[0]
            content = chunk[1]

        if not key and not content:
            return

        chunk = self.parse_value(content)
        value = chunk[0]
        content = chunk[1]

        self.data[key] = value

        while len(content) != 0:
            if content[0] == ',':
                content = content[1:]
            else:
                break

            chunk = self.parse_key(content)
            key = chunk[0]
            content = chunk[1]
            if len(content) > 0:
                chunk = self.parse_value(content)
                value = chunk[0]
                content = chunk[1]
                self.data[key] = value
            else:
                self.data[key] = None

    def parse_first_key(self, content):
        """
         Consume the first field key, which could be a class name.
              :param content str The input to consume
              :return: list The collected string and any remaining content,
               followed by a boolean indicating whether this is a class name.
        """
        length = len(content)
        collected = ''
        is_class_name = False
        if content[0] == '"':
            result = self.parse_string(content[1:])
            return [result[0], result[1][1:]]

        i = 0
        for i in range(0, length):
            c = content[i]
            if c == '@':
                is_class_name = True
                break
            elif c == ':':
                break
            else:
                collected += c

        return [collected, content[( i + 1 ):], is_class_name]

    def parse_key( self, content ):
        """
          Consume a field key, which may or may not be quoted.
            :param content str The input to consume
            :return: dict The collected string and any remaining content.
        """
        length = len(content)
        collected = ''
        if content[ 0 ] == '"':
            result = self.parse_string( content[1:] )
            return [ result[ 0 ], result[1][1:] ]

        i = 0
        for i in range(0, length):
            c = content[i]
            if c == ':':
                break
            else:
                collected += c

        return [ collected, content[( i + 1 ):] ]

    def parse_value( self, content ):
        """
          Consume a field value.
            :param: content str The input to consume
            :return: list The collected value and any remaining content.
        """
        c = ''
        content = content.lstrip( " " )
        try:
            c = content[ 0 ]  # string index out of range 0
        except IndexError:
            pass

        if len( content ) == 0 or c == ',':
            return [ None, content ]
        elif c == '"':
            return self.parse_string( content[1:] )
        elif c == '#':
            return self.parse_rid( content[1:] )
        elif c == '[':
            return self.parse_collection( content[1:] )
        elif c == '<':
            return self.parse_set( content[1:] )
        elif c == '{':
            return self.parse_map( content[1:] )
        elif c == '(':
            return self.parse_record( content[1:] )
        elif c == '%':
            return self.parse_bag( content[1:] )
        elif c == '_':
            return self.parse_binary( content[1:] )
        elif c == '-' or self.is_numeric( c ):
            return self.parse_number( content )
        elif c == 'n' and content[ 0:4 ] == 'null':
            return [ None, content[ 4: ] ]
        elif c == 't' and content[ 0:4 ] == 'true':
            return [ True, content[ 4: ] ]
        elif c == 'f' and content[ 0:5 ] == 'false':
            return [ False, content[ 5: ] ]
        else:
            return [ None, content ]

    @staticmethod
    def is_numeric( content ):
        try:
            float( content )
            return True
        except ValueError:
            return False

    @staticmethod
    def parse_string( content ):
        """
          Consume a string.
            :param content str The input to consume
            :return: list The collected string and any remaining content.
        """
        length = len( content )
        collected = ''
        i = 0
        while i < length:
            c = content[ i ]
            if c == '\\':
                # escape, skip to the next character
                i += 1
                collected += content[ i ]
                # increment again to pass over
                i += 1
                continue
            elif c == '"':
                break
            else:
                i += 1
                collected += c

        return [ collected, content[ ( i + 1 ): ] ]

    def parse_number(self, content):
        """
           Consume a number.
           If the number has a suffix, consume it also and instantiate the
            right type, e.g. for dates
           :param content str The content to consume
           :return: list The collected number and any remaining content.
        """
        length = len(content)
        collected = ''
        is_float = False
        i = 0
        for i in range(0, length):
            c = content[i]
            if c == '-' or self.is_numeric(c):
                collected += c
            elif c == '.':
                is_float = True
                collected += c
            else:
                break

        content = content[i:]
        c = ''
        try:
            c = content[ 0 ]  # string index out of range 0
        except IndexError:
            pass

        if c == 'a':
            collected = date.fromtimestamp(float(collected) / 1000)
            content = content[1:]
        elif c == 't':
            # date
            collected = datetime.fromtimestamp(float(collected) / 1000)
            content = content[1:]
        elif c == 'f':
            # float
            collected = float(collected)
            content = content[1:]
        elif c == 'b' or c == 's' or c == 'l':
            collected = int(collected)
            content = content[1:]
        elif c == 'c' or c == 'd':
            content = content[1:]
        elif is_float:
            collected = float(collected)
        else:
            collected = int(collected)

        return [collected, content]

    def parse_rid(self, content):
        """
          Consume a Record ID.
          :param content str The input to consume
          :return: list The collected RID and any remaining content.
        """
        length = len(content)
        collected = ''
        cluster = None
        i = 0
        for i in range(0, length):
            c = content[i]
            if cluster is None and c == ':':
                cluster = collected
                collected = ''
            elif self.is_numeric(c):
                collected += c
            else:
                break

        return [ OrientRecordLink( cluster + ":" + collected ), content[i:]]

    def parse_collection(self, content):
        """
            Consume an array of values.
            :param content str The input to consume
            :return: list The collected array and any remaining content.
        """
        collection = []
        while len(content) != 0:
            c = content[0]
            if c == ',':
                content = content[1:]
            elif c == ']':
                content = content[1:]
                break

            chunk = self.parse_value(content)
            collection.append(chunk[0])
            content = chunk[1]

        return [collection, content]

    def parse_set(self, content):
        """
           Consume a set of values.
           :param content str The input to consume
           :return: list The collected set and any remaining content.
        """
        list_set = []
        while len(content) != 0:
            c = content[0]
            if c == ',':
                content = content[1:]
            elif c == '>':
                content = content[1:]
                break

            chunk = self.parse_value(content)
            list_set.append(chunk[0])
            content = chunk[1]

        return [list_set, content]

    def parse_map( self, content ):
        """
            Consume a map of keys to values.
            :param content str The input to consume
            :return: list The collected map and any remaining content.
        """
        _map = {}
        content = content.lstrip(' ')
        while len(content) != 0:
            c = content[0]
            if c == ' ':
                content = content[1:].lstrip(' ')
                continue
            elif c == ',':
                content = content[1:].lstrip(' ')
            elif c == '}':
                content = content[1:]
                break

            chunk = self.parse_key(content)
            key = chunk[0]
            content = chunk[1].lstrip(' ')
            if len(content) != 0:
                chunk = self.parse_value(content)
                _map[key] = chunk[0]
                content = chunk[1].lstrip(' ')
            else:
                _map[key] = None
                break

        return [_map, content]

    def parse_record(self, content):
        """
          Consume an embedded record.
           :param content str The content to unserialize.
           :return: list The collected record and any remaining content.
        """
        record = {}

        content = content.lstrip(' ')
        if content[0] == ')':
            # this is an empty record.
            return [record, content[1:]]

        chunk = self.parse_first_key(content)
        if chunk[2]:
            # this is actually a class name.
            record['o_class'] = chunk[0]
            content = chunk[1].lstrip(' ')
            if content[0] == ')':
                return [record, content[1:]]

            chunk = self.parse_key(content)
            key = chunk[0]
            content = chunk[1]
        else:
            key = chunk[0]
            content = chunk[1]

        chunk = self.parse_key(content)
        value = chunk[0]
        content = chunk[1].lstrip(' ')

        record[key] = value

        while len(content) > 0:
            if content[0] == ',':
                content = content[1:].lstrip(' ')
            elif content[0] == ')':
                content = content[1:].lstrip(' ')
                break

            chunk = self.parse_key(content)
            key = chunk[0]
            content = chunk[1].lstrip(' ')
            if len(content) > 0:
                chunk = self.parse_value(content)
                value = chunk[0]
                content = chunk[1]
                record[key] = value
            else:
                record[key] = None

        return [record, content]

    @staticmethod
    def parse_bag(content):
        """
         Consume a record id bag.
           :param content str The content to consume
           :return: list The collected record id bag and any remaining content.
        """
        length = len(content)
        collected = ''
        i = 0
        for i in range(0, length):
            c = content[i]
            if c == ';':
                break
            else:
                collected += c

        return [OrientBinaryObject(collected), content[( i + 1 ):]]

    @staticmethod
    def parse_binary(content):
        """
          Consume a binary field.
            :param content str The content to consume
            :return: list The collected binary and any remaining content.
        """
        length = len(content)
        collected = ''
        i = 0
        for i in range(0, length):
            c = content[i]
            if c == '_' \
                    or c == ',' \
                    or c == ')' \
                    or c == '>' \
                    or c == '}' \
                    or c == ']':
                break
            else:
                collected += c

        return [collected, content[( i + 1 ):]]


class ORecordEncoder(object):
    """docstring for ORecordEncoder"""

    def __init__(self, oRecord):
        self._raw = self.__encode(oRecord)

    def __encode(self, record):

        raw = ''
        o_class = getattr(record, 'o_class', False)
        if o_class:
            raw = o_class + '@'

        fields = list(filter(lambda item: not item.startswith('_OrientRecord_'),
                             record.oRecordData))

        for idx, key in enumerate(fields):
            raw += key + ':'
            value = record.oRecordData[key]
            raw += self.parse_value(value)

            if idx < len(list(fields)) - 1:
                # not last element
                raw += ','

        return raw

    def parse_value(self, value):

        if isinstance(value, str):
            ret = '"' + value + '"'
        elif isinstance(value, float):
            ret = str(value) + 'f'

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
            ret = str(int(time.mktime(value.timetuple()))) + 't'
        elif isinstance(value, date):
            ret = str(int(time.mktime(value.timetuple())) * 1000) + 'a'
        elif isinstance(value, list):
            try:
                ret = "[" + ','.join(
                    map(lambda elem: self.parse_value(type(value[0])(elem)),
                        value)) + ']'
            except ValueError as e:
                raise Exception("wrong type commistion")
        elif isinstance(value, dict):
            ret = "{" + ','.join(map(
                lambda elem: '"' + elem + '":' + self.parse_value(value[elem]),
                value)) + '}'
        elif isinstance(value, OrientRecord):
            ret = "(" + self.__encode(value) + ")"
        elif isinstance(value, OrientRecordLink):
            ret = value.get_hash()
        elif isinstance(value, OrientBinaryObject):
            ret = value.getRaw()
        else:
            ret = ''
        return ret

    def get_raw(self):
        return self._raw