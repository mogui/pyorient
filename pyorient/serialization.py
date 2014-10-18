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
from datetime import date, datetime
from .types import OrientRecordLink, OrientRecord, OrientBinaryObject
from .utils import is_debug_active


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
                        record.__dict__))

        for idx, key in enumerate(fields):
            raw += key + ':'
            value = getattr(record, key)
            raw += self.parse_value(value)

            if idx < len(list(fields)) - 1:
                # not last element
                raw += ','

        return raw

    def parse_value(self, value):

        if isinstance(value, str):
            ret = '"' + value + '"'
        elif isinstance(value, int):
            ret = str(value)
        elif isinstance(value, float):
            ret = str(value) + 'f'
        elif isinstance(value, long):
            ret = str(value) + 'l'
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
        self._isCollection = False
        self._isMap = False
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
                    token_type == TTYPE_MAP_START:
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
                if not self._isCollection and not self._isMap:
                    tt, t_value = self.__stack_pop()
                    tt, t_name = self.__stack_pop()
                    #print("%s -> %s" % (tname, tvalue))
                    self.data[t_name] = t_value

            elif token_type == TTYPE_NULL:
                if not self._isCollection and not self._isMap:
                    self.__stack_pop()
                    tt, t_name = self.__stack_pop()
                    self.data[t_name] = None

            elif token_type == TTYPE_COLLECTION_END:
                values = []
                while True:
                    search_token, value = self.__stack_pop()
                    if search_token != TTYPE_COLLECTION_START and \
                                    search_token != TTYPE_COLLECTION_END:
                        values.append(value)
                    if search_token == TTYPE_COLLECTION_START:
                        break
                tt, t_name = self.__stack_pop()
                values.reverse()
                self.data[t_name] = values

            elif token_type == TTYPE_MAP_END:
                values = {}
                while True:
                    search_token, value = self.__stack_pop()
                    if search_token == TTYPE_NULL:
                        value = None
                    if search_token != TTYPE_MAP_START and \
                                    search_token != TTYPE_MAP_END:
                        tt, key = self.__stack_pop()
                        values[key] = value
                    if search_token == TTYPE_MAP_START:
                        break

                tt, t_name = self.__stack_pop()
                self.data[t_name] = values
            else:
                #print("orly?")
                pass

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
            self._state = STATE_COMMA
            # token type is collection end
            self.__stack_push(TTYPE_COLLECTION_END)
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
            # check if null value in the end of the map
            if self.__stack_get_last_type() == TTYPE_KEY:
                # token type is map end
                self.__stack_push(TTYPE_NULL)
                return

            self._state = STATE_COMMA
            # token type is map end
            self.__stack_push(TTYPE_MAP_END)
            # stopped map
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

        elif char == 'f' or char == 't':
            # boolean found - switch state to boolean
            self._state = STATE_BOOLEAN
            self._buffer = char
            self._i += 1
        elif char == '%':
            self._state = STATE_BASE64
            self._i += 1
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
            if self._isCollection:
                self._state = STATE_VALUE
            elif self._isMap:
                self._state = STATE_KEY
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
            #switch state to
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
            elif char == 'f' or char == 'd':
                token_value = float(self._buffer)
            elif char == 't':
                token_value = datetime.fromtimestamp(float(self._buffer))
            elif char == 'a':
                token_value = date.fromtimestamp(float(self._buffer) / 1000)
            else:
                token_value = int(self._buffer)

            #token type is a number
            self.__stack_push(TTYPE_NUMBER, token_value)

    def __state_key(self, char, c_class):
        """docstring for __state_key"""
        if char == ":":
            self._state = STATE_VALUE
            self.__stack_push(TTYPE_KEY)
        else:
            # Fast-forwarding to " symbol
            if self._i < len(self.content):
                pos = self.content.find('"', self._i + 1)
            else:
                pos = False

            if pos != False and pos > self._i:
                # Before " symbol
                self._buffer = self.content[self._i + 1:pos]
                self._i = pos
        self._i += 1

    def __state_boolean(self, char, c_class):
        """docstring for __state_boolean"""
        token_value = False
        if self.content[self._i:].find('rue') == self._i:
            token_value = True
            self._i += 3
        elif self.content[self._i:].find('alse') == self._i:
            token_value = False
            self._i += 4
        else:
            # @TODO raise an exception
            pass
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
        return self._stackTokenTypes.pop(), self._stackTokenValues.pop()

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
