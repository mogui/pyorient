from __future__ import print_function

# Copyright 2012 Niko Usai <usai.niko@gmail.com>, http://mogui.it
#
#   this file is part of pyorient
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# @BUG nested list e dict non funzionano nel parser

import re
import time
import sys
from datetime import date, datetime
from .types import OrientRecordLink, OrientRecord, OrientBinaryObject
from .utils import is_debug_active
from .exceptions import PyOrientSerializationException


# what we are going to collect
STATE_GUESS = 0
STATE_NAME = 1
STATE_VALUE = 2
STATE_STRING = 3
STATE_COMMA = 4
STATE_LINK = 5
STATE_NUMBER = 6
STATE_KEY = 7
STATE_BOOLEAN = 8
STATE_BUFFER = 9
STATE_BASE64 = 10


#character classes
CCLASS_WORD = 1
CCLASS_NUMBER = 2
CCLASS_OTHER = 0

TTYPE_NAME = 0
TTYPE_CLASS = 1
TTYPE_NULL = 2
TTYPE_STRING = 3
TTYPE_COLLECTION_START = 4
TTYPE_COLLECTION_END = 5
TTYPE_LINK = 6
TTYPE_NUMBER = 7
TTYPE_MAP_START = 8
TTYPE_MAP_END = 9
TTYPE_BOOLEAN = 10
TTYPE_KEY = 11
TTYPE_EMBEDDED = 12
TTYPE_BUFFER = 13
TTYPE_BASE64 = 14
TTYPE_LINKSET_START = 15
TTYPE_LINKSET_END = 16


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


class ORecordDecoder(object):
    """Porting of PHP OrientDBRecordDecoder"""

    def __init__(self, content):
        # public
        self.className = None
        self.content = content
        self.data = {}

        # private
        self._state = STATE_GUESS
        self._buffer = ''
        self._continue = True
        self._i = 0
        self._stackTokenValues = []
        self._stackTokenTypes = []
        self._previousStackTokenType = -1
        self._isCollection = False
        self._isMap = False
        self._isLinkSet = False
        self.escape = False
        self._stateCase = [self.__state_guess, self.__state_name,
                           self.__state_value, self.__state_string,
                           self.__state_comma, self.__state_link,
                           self.__state_number, self.__state_key,
                           self.__state_boolean, self.__state_buffer,
                           self.__state_base64]

        # start decoding
        self.__decode()

        if self.__stack_get_last_type():
            # Bug if the number is the last value of string
            self._stateCase[self._state](",", None )
            try:
                tt, t_value = self.__stack_pop()
                tt, t_name = self.__stack_pop()
                self.data[t_name] = t_value
            except Exception as e:
                if is_debug_active():
                    # hexdump( self._output_buffer.decode() )
                    print("\nException Raised:")
                    print(repr(e.message))

    def __decode(self):
        """docstring for decode"""

        if not isinstance(self.content, str):
            self.content = self.content.decode()

        while self._i < len(self.content) and self._continue:
            char = self.content[self._i:self._i + 1]
            c_code = ord(char)
            if (65 <= c_code <= 90) or \
                    (97 <= c_code <= 122) or c_code == 95:
                c_class = CCLASS_WORD
            elif 48 <= c_code <= 57:
                c_class = CCLASS_NUMBER
            else:
                c_class = CCLASS_OTHER

            # pythonic switch case
            self._stateCase[self._state](char, c_class)
            token_type = self.__stack_get_last_type()

            if token_type == TTYPE_NAME or \
                    token_type == TTYPE_KEY or \
                    token_type == TTYPE_COLLECTION_START or \
                    token_type == TTYPE_MAP_START or \
                    token_type == TTYPE_LINKSET_START:
                pass

            elif token_type == TTYPE_CLASS:   # TTYPE_CLASS = 1
                (t_type, t_value) = self.__stack_pop()
                self.className = t_value

            elif token_type == TTYPE_NUMBER or \
                    token_type == TTYPE_STRING or \
                    token_type == TTYPE_BUFFER or \
                    token_type == TTYPE_BOOLEAN or \
                    token_type == TTYPE_EMBEDDED or \
                    token_type == TTYPE_BASE64 or \
                    token_type == TTYPE_LINK:
                if not self._isCollection and not self._isMap \
                        and not self._isLinkSet:
                    tt, t_value = self.__stack_pop()
                    tt, t_name = self.__stack_pop()
                    # print("%s -> %s" % (tname, tvalue))
                    self.data[t_name] = t_value

            elif token_type == TTYPE_NULL:
                if not self._isCollection and not self._isMap:
                    self.__stack_pop()
                    tt, t_name = self.__stack_pop()
                    self.data[t_name] = None

            elif token_type == TTYPE_COLLECTION_END \
                    or token_type == TTYPE_MAP_END:
                if self._isCollection or self._isMap:
                    # we are in a nested collection/map, next cycle
                    continue

                self._stackTokenTypes.reverse()
                self._stackTokenValues.reverse()
                self.data = self.__reduce_maps( self.data )
                pass

            elif token_type == TTYPE_LINKSET_END:
                listSet = []
                while self.__stack_get_last_type() != TTYPE_KEY:
                    tt, t_value = self.__stack_pop()
                    if tt != TTYPE_LINKSET_END and tt != TTYPE_LINKSET_START:
                        listSet.append( t_value )
                tt, t_name = self.__stack_pop()
                # print("%s -> %s" % (tname, tvalue))
                self.data[t_name] = listSet
                pass

            else:
                #print("orly?")
                pass

    def __reduce_maps(self, container):

        while len(self._stackTokenTypes):

            previous_token = self._previousStackTokenType
            actual_token = self.__stack_get_last_type()

            # we are in this situation:
            # [1,{"dx":[1,2]},"abc"]
            # container = {"dx":[1,2]}
            # and next_token is a string "abc",
            # it must be added to the parent object
            if actual_token == TTYPE_NULL and previous_token in [
                TTYPE_MAP_END,
                TTYPE_COLLECTION_END
            ]:
                self.__stack_pop()

            # now pop from stack and take new values
            # actual_token will be equals to incoming_token
            actual_token, actual_value = self.__stack_pop()

            # look to the next, we need to know if
            # the next element is not a scalar.
            # it can never be None
            next_token = self.__stack_get_last_type()

            # this is an empty collection/dict
            if (
                    actual_token == TTYPE_MAP_START and next_token == TTYPE_MAP_END) \
                    or ( actual_token == TTYPE_COLLECTION_START
                         and next_token == TTYPE_COLLECTION_END ):
                return []

            if actual_token == TTYPE_NULL:
                # field separator "," inside a list
                continue

            # ok this is an element
            if actual_token not in [
                TTYPE_COLLECTION_START,
                TTYPE_COLLECTION_END,
                TTYPE_MAP_START,
                TTYPE_MAP_END
            ]:
                # after this element there are a list/dict
                if next_token in [TTYPE_COLLECTION_START, TTYPE_MAP_START]:

                    # choose the right container
                    next_container = {} if next_token == TTYPE_MAP_START else []

                    # we need to know if we are in a collection or in a map
                    if actual_token == TTYPE_KEY:
                        self.__stack_pop()
                        container[actual_value] = \
                            self.__reduce_maps( next_container )
                    else:
                        container.append(actual_value)

                else:
                    # this element is a dict key type?
                    # try to add to the result container
                    if actual_token == TTYPE_KEY:
                        tt, value = self.__stack_pop()
                        if actual_value != '':
                            container[actual_value] = value
                    else:
                        container.append(actual_value)

            elif actual_token == TTYPE_COLLECTION_START:
                if isinstance(container, dict):
                    container[actual_value] = self.__reduce_maps([])
                else:
                    container.append(self.__reduce_maps([]))

            elif actual_token == TTYPE_MAP_START:
                if isinstance(container, dict):
                    container[actual_value] = self.__reduce_maps({})
                else:
                    container.append(self.__reduce_maps({}))

            elif actual_token in [TTYPE_COLLECTION_END, TTYPE_MAP_END]:
                break

        return container

    def __state_guess(self, char, c_class):
        """docstring for guess"""
        self._state = STATE_NAME
        self._buffer = char
        self._i += 1

    def __state_name(self, char, c_class):
        """docstring for name"""
        if char == ':':
            self._state = STATE_VALUE
            self.__stack_push(TTYPE_KEY)
        elif char == '@':
            self.__stack_push(TTYPE_CLASS)
        else:
            # trying to fast-forward name collecting @TODO
            self._buffer += char

        self._i += 1

    def __state_value(self, char, c_class):
        """docstring for __state_value"""
        if char == ',':
            # No value - switch state to comma
            self._state = STATE_COMMA
            # token type is null
            self.__stack_push(TTYPE_NULL)
        elif char == '"':
            # switch state to string collecting
            self._state = STATE_STRING
            self._i += 1
        elif char == '_':
            # switch state to string collecting
            self._state = STATE_BUFFER
            self._i += 1
        elif char == '#':
            # found hash - switch state to link
            self._state = STATE_LINK
            # add hash to value
            self._buffer = char
            self._i += 1
        elif char == '[':
            # [ found, state is still value
            self._state = STATE_VALUE
            # token type is collection start
            self.__stack_push(TTYPE_COLLECTION_START)
            # started collection
            self._isCollection = True
            self._i += 1
        elif char == ']':
            # ] found,
            # before to stop we have to know if there are
            # other nested collections
            # token type is collection end
            self.__stack_push(TTYPE_COLLECTION_END)
            self._state = STATE_COMMA
            if self._stackTokenTypes.count(TTYPE_COLLECTION_START) == \
                    self._stackTokenTypes.count(TTYPE_COLLECTION_END):
                # stopped collection
                self._isCollection = False
            self._i += 1
        elif char == '{':
            # found { switch state to name
            self._state = STATE_KEY
            # token type is map start
            self.__stack_push(TTYPE_MAP_START)
            # started map
            self._isMap = True
            self._i += 1
        elif char == '}':
            # } found
            # check for null value in the end of the map
            if self.__stack_get_last_type() == TTYPE_KEY:
                # token type is NULL
                self.__stack_push(TTYPE_NULL)
                return

            # before to stop we have to know if there are
            # other nested maps
            # token type is map end
            self.__stack_push(TTYPE_MAP_END)
            self._state = STATE_COMMA
            if self._stackTokenTypes.count(TTYPE_MAP_START) == \
                    self._stackTokenTypes.count(TTYPE_MAP_END):
                # stopped collection
                self._isMap = False

            self._i += 1
        elif char == '(':
            # ( found, state is COMMA
            self._state = STATE_COMMA
            # increment position so we can transfer clean document
            self._i += 1
            parser = ORecordDecoder(self.content[self._i:])
            rec = OrientRecord(parser.data,
                               o_class=parser.className)

            token_value = rec
            # token type is embedded
            self.__stack_push(TTYPE_EMBEDDED, token_value)
            # fast forward to embedded position
            self._i += parser._i
            # increment counter so we can continue on clean document
            self._i += 1

        elif char == ')':
            # end of current document reached
            self._continue = False

        elif char == '<':
            # [ found, state is still value
            self._state = STATE_VALUE
            # token type is linkset start
            self.__stack_push(TTYPE_LINKSET_START)
            # started linkset
            self._isLinkSet = True
            self._i += 1

        elif char == '>':
            # > found,
            self.__stack_push(TTYPE_LINKSET_END)
            self._state = STATE_COMMA
            self._isLinkSet = False
            self._i += 1

        elif char == 'f' or char == 't':
            # boolean found - switch state to boolean
            self._state = STATE_BOOLEAN
            self._buffer = char
            self._i += 1
        elif char == '%':
            self._state = STATE_BASE64
            self._i += 1
        elif char == 'n' and self.content[self._i:self._i + 4] == 'null':
            self._state = STATE_NUMBER
            self._buffer = self.content[self._i:self._i + 4]
        else:
            if c_class == CCLASS_NUMBER or char == '-':
                # number found - switch to number collecting
                self._state = STATE_NUMBER
                self._buffer = char
                self._i += 1
            elif char is False:
                self._i += 1
                # end __state_value()

    def __state_buffer(self, char, oClass):
        pos_end = self.content[self._i:].find('_')
        if pos_end > 1:
            self._buffer = self.content[self._i:(self._i + pos_end)]
            self._i += pos_end - 1

        if char == '_':
            self._state = STATE_COMMA
            self.__stack_push(TTYPE_BUFFER, OrientBinaryObject(self._buffer))
        self._i += 1

    def __state_string(self, char, c_class):
        if self._i < len(self.content):
            pos_quote = self.content[self._i:].find('"')
            pos_escape = self.content[self._i:].find('\\')

            if pos_escape != -1:
                pos = min(pos_quote, pos_escape)
            else:
                pos = pos_quote
        else:
            pos = False

        if pos and pos > 1:
            self._buffer += self.content[self._i:(self._i + pos)]
            self._i += pos
            return

        if char == '\\':
            if self.escape:
                self._buffer += char
                self.escape = False
            else:
                self.escape = True
        elif char == '"':
            if self.escape:
                self._buffer += char
                self.escape = False
            else:
                self._state = STATE_COMMA
                self.__stack_push(TTYPE_STRING)
        else:
            self._buffer += char

        self._i += 1

    def __state_base64(self, char, c_class):
        pos_end = self.content[self._i:].find(';')
        self._buffer += self.content[self._i:(self._i + pos_end)]
        self._i += pos_end + 1              # skip th semi colon
        self._state = STATE_COMMA
        self.__stack_push(TTYPE_BASE64, OrientBinaryObject(self._buffer) )

    def __state_comma(self, char, c_class):
        """docstring for __state_comma"""
        if char == ',':

            # Here we can't know if it is a map or list
            # when one is inside the other,
            # so check backward until the first opening type
            # TODO: Delete this old and sick CSV parser and write
            # TODO: another recursive one by myself
            if self._isMap and self._isCollection:
                for token in self._stackTokenTypes[::-1]:  # reverse the list
                    if token == TTYPE_MAP_START:
                        self._state = STATE_KEY
                        break
                    elif token == TTYPE_COLLECTION_START:
                        self.__stack_push(TTYPE_NULL)
                        self._state = STATE_VALUE
                        break

            elif self._isCollection:
                self.__stack_push(TTYPE_NULL)
                self._state = STATE_VALUE
            elif self._isMap:
                self._state = STATE_KEY
            elif self._isLinkSet:
                self._state = STATE_VALUE
            else:
                self._state = STATE_GUESS
            self._i += 1
        else:
            self._state = STATE_VALUE

    def __state_link(self, char, c_class):
        """docstring for __state_link"""
        result = re.search('\d+:\d+', self.content[self._i:], re.I)
        if result and result.start() == 0:
            self._buffer = result.group()
            self._i += len(result.group())
        else:
            if char == ',':
                self._state = STATE_COMMA
            else:
                self._state = STATE_VALUE

            self.__stack_push(TTYPE_LINK, OrientRecordLink(self._buffer))

    def __state_number(self, char, c_class):
        """docstring for __state_number"""
        result = re.search('[\d\.e-]+', self.content[self._i:], re.I)
        if result and result.start() == 0:
            self._buffer += result.group()
            self._i += len(result.group())
        else:
            # switch state to
            if char == ',':
                self._state = STATE_COMMA
            elif c_class == CCLASS_WORD:
                self._state = STATE_COMMA
                self._i += 1
            else:
                self._state = STATE_VALUE
            # fill token
            if char == 'b' or char == 's':
                token_value = int(self._buffer)
            elif char == 'l':
                token_value = int(self._buffer)
            elif char == 'f' or char == 'd' or char == 'c':
                token_value = float(self._buffer)
            elif char == 'a':
                token_value = date.fromtimestamp(float(self._buffer) / 1000)
            elif char == 't':
                token_value = datetime.fromtimestamp(float(self._buffer) / 1000)
            elif char == 'n':
                token_value = None
                self._i += 3
            else:
                token_value = int(self._buffer)

            # token type is a number
            self.__stack_push(TTYPE_NUMBER, token_value)

    def __state_key(self, char, c_class):
        """docstring for __state_key"""
        if char == ":":
            self._state = STATE_VALUE
            self.__stack_push(TTYPE_KEY)
        elif char == '}':
            # here a key is expected, but
            # try to check if this is an empty dict '{}'
            self._state = STATE_COMMA
            self.__stack_push(TTYPE_MAP_END)
            self._isMap = False
        else:
            # Fast-forwarding to " symbol
            if self._i < len(self.content):
                pos = self.content.find('"', self._i + 1)
            else:
                pos = False

            if pos is not False and pos > self._i:
                # Before " symbol
                self._buffer = self.content[self._i + 1:pos]
                self._i = pos

        self._i += 1

    def __state_boolean(self, char, c_class):
        """docstring for __state_boolean"""
        token_value = False

        # self._i is the position in the result string of the first letter of
        # the boolean ( [f]alse/[t]true )
        # 'V@abcdef:false' -> self._i == 10
        if self.content[self._i:self._i + 3] == 'rue':
            token_value = True
            self._i += 3
        elif self.content[self._i:self._i + 4] == 'alse':
            token_value = False
            self._i += 4
        else:
            raise PyOrientSerializationException( 'Invalid boolean read', [] )

        self._state = STATE_COMMA
        self.__stack_push(TTYPE_BOOLEAN, token_value)

    def __stack_push(self, token_type, token_value=None):
        """docstring for __stack_push"""
        self._stackTokenTypes.append(token_type)
        if token_value is None:
            token_value = self._buffer

        self._stackTokenValues.append(token_value)
        self._buffer = ''

    def __stack_pop(self):
        """ pop value from stack """
        t_type = self._stackTokenTypes.pop()
        self._previousStackTokenType = t_type
        return t_type, self._stackTokenValues.pop()

    def __stack_get_last_type(self):
        """docstring for __stack_get_last_type"""
        if len(self._stackTokenTypes) > 0:
            return self._stackTokenTypes[-1]
        else:
            return None

    def __stack_get_last_key(self):
        """ returns last inserted value"""
        depth = False

        for i in range(len(self._stackTokenTypes) - 1, -1, -1):
            if self._stackTokenTypes[i] == TTYPE_NAME:
                depth = i
                break

        if depth is not False:
            return self._stackTokenValues[depth]
