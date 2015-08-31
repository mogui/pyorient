import sys
import time
from datetime import date, datetime
from decimal import Decimal


class OrientRecord(object):
    """
    Object that represent an Orient Document / Record

    """
    oRecordData = property(lambda self: self.__o_storage)

    def __str__(self):
        rep = ""
        if self.__o_storage:
            rep = str( self.__o_storage )
        if self.__o_class is not None:
            rep = "'@" + str(self.__o_class) + "':" + rep + ""
        if self.__version is not None:
            rep = rep + ",'version':" + str(self.__version)
        if self.__rid is not None:
            rep = rep + ",'rid':'" + str(self.__rid) + "'"
        return '{' + rep + '}'

    def __init__(self, content=None):

        self.__rid = None
        self.__version = None
        self.__o_class = None
        self.__o_storage = {}

        if not content:
            content = {}
        for key in content.keys():
            if key == '__rid':  # Ex: select @rid, field from v_class
                self.__rid = content[ key ]
                # self.__rid = OrientRecordLink( content[ key ][ 1: ] )
            elif key == '__version':  # Ex: select @rid, @version from v_class
                self.__version = content[key]
            elif key == '__o_class':
                self.__o_class = content[ key ]
            elif key[0:1] == '@':
                # special case dict
                # { '@my_class': { 'accommodation': 'hotel' } }
                self.__o_class = key[1:]
                for _key, _value in content[key].items():
                    self.__o_storage[_key] = _value
            elif key == '__o_storage':
                self.__o_storage = content[key]
            else:
                self.__o_storage[key] = content[key]

    def _set_keys(self, content=dict):
        for key in content.keys():
                self._set_keys( content[key] )

    @property
    def _in(self):
        try:
            return self.__o_storage['in']
        except KeyError:
            return None

    @property
    def _out(self):
        try:
            return self.__o_storage['out']
        except KeyError:
            return None

    @property
    def _rid(self):
        return self.__rid

    @property
    def _version(self):
        return self.__version

    @property
    def _class(self):
        return self.__o_class

    def update(self, **kwargs):
        self.__rid = kwargs.get('__rid', None)
        self.__version = kwargs.get('__version', None)
        if self.__o_class is None:
            self.__o_class = kwargs.get('__o_class', None)

    """ This method is for backward compatibility when someone
        use 'getattr(record, a_key)' """
    def __getattr__(self, item):
        """
        :param item: string
        :return: mixed
        :raise: AttributeError
        """
        try:
            return self.__o_storage[item]
        except KeyError:
            raise AttributeError( "'OrientRecord' object has no attribute "
                                  "'" + item + "'" )


class OrientRecordLink(object):
    def __init__(self, recordlink):
        cid, rpos = recordlink.split(":")
        self.__link = recordlink
        self.clusterID = cid
        self.recordPosition = rpos

    def __str__(self):
        return self.get_hash()

    def get(self):
        return self.__link

    def get_hash(self):
        return "#%s" % self.__link


class OrientBinaryObject(object):
    """
    This will be a RidBag
    """
    def __init__(self, stri):
        self.b64 = stri

    def getRaw(self):
        return "_" + self.b64 + "_"

    def getBin(self):
        import base64
        return base64.b64decode(self.b64)


class OrientCluster(object):
    def __init__(self, name, cluster_id, cluster_type=None, segment=None):
        """
        Information regarding a Cluster on the Orient Server
        :param name: str name of the cluster
        :param id: int id of the cluster
        :param type: cluster type (only for version <24 of the protocol)
        :param segment: cluster segment (only for version <24 of the protocol)
        """
        #: str name of the cluster
        self.name = name
        #: int idof the cluster
        self.id = cluster_id
        self.type = cluster_type
        self.segment = segment

    def __str__(self):
        return "%s: %d" % (self.name, self.id)

    def __eq__(self, other):
        return self.name == other.name and self.id == other.id

    def __ne__(self, other):
        return self.name != other.name or self.id != other.id


class OrientNodeList(object):
    def __init__(self, nodelist, host, port ):
        self.listeners = []

        _locals = [ "127.0.0.1", "localhost" ]
        if host in _locals:
            host = _locals[0]
        try:
            for member in nodelist.data['members']:
                _lst = [ listener for listener in member['listeners']
                         if listener['protocol'] == 'ONetworkProtocolBinary' ][0]
                item = _lst['listen'].split( ':' )

                if item[0] in _locals:
                    item[0] = _locals[0]

                # skip this address from list
                if item[0] == host and item[1] == str(port):
                    continue

                self.listeners.append( { 'address': item[0], 'port': item[1] } )
        except KeyError:
            pass


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
        if length == 0:
            return [None, None]
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
            elif c == 'E' and is_float:
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
        elif c == 'f' or c == 'd':
            # float # double
            collected = float(collected)
            content = content[1:]
        elif c == 'c':
            collected = Decimal(collected)
            content = content[1:]
        elif c == 'b' or c == 's':
            collected = int(collected)
            content = content[1:]
        elif c == 'l':
            if sys.version_info[0] < 3:
                collected = long(collected)  # python 2.x long type
            else:
                collected = int(collected)
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

        fields = list(record.oRecordData)

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
            ret = str(int(time.mktime(value.timetuple())) * 1000) + 't'
        elif isinstance(value, date):
            ret = str(int(time.mktime(value.timetuple())) * 1000) + 'a'
        elif isinstance(value, Decimal):
            ret = str(value) + 'c'
        elif isinstance(value, list):
            try:
                ret = "[" + ','.join(
                    map(
                        lambda elem: self.parse_value(type(value[0])(elem))
                        if not isinstance(value[0], OrientRecordLink)
                        else elem.get_hash(),
                        value
                    )) + ']'
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


class OrientVersion(object):

    def __init__(self, release):
        """
        Object representing Orient db release Version

        :param release: String release
        """

        #: string full OrientDB release
        self.release = release

        #: Major version
        self.major = None

        #: Minor version
        self.minor = None

        #: build number
        self.build = None

        self._parse_version(release)

    def _parse_version( self, string_release ):

        if not isinstance(string_release, str):
            string_release = string_release.decode()

        try:
            version_info = string_release.split( "." )
            self.major = int( version_info[0] )
            self.minor = version_info[1]
            self.build = version_info[2]
        except IndexError:
            pass

        if "-" in self.minor:
            _temp = self.minor.split( "-" )
            self.minor = int( _temp[0] )
            self.build = _temp[1]
        else:
            self.minor = int( self.minor )

        build = self.build.split( " ", 1 )[0]
        try:
            build = int( build )
        except ValueError:
            pass

        self.build = build

    def __str__(self):
        return self.release


class OrientNode(object):
    def __init__(self, node_dict=None):
        """
        Represent a server node in a multi clusered configuration

        TODO: extends this object with different listeners if we're going to support in the driver an abstarction of the HTTP protocol, for now we are not interested in that

        :param node_dict: dict with starting configs (usaully from a db_open, db_reload record response)
        """
        #: node name
        self.name = None

        #: node is
        self.id = None

        #: datetime object the node was started
        self.started_on = None

        #: binary listener host
        self.host = None

        #: binary lister port
        self.port = None

        if node_dict is not None:
            self._parse_dict(node_dict)

    def _parse_dict(self, node_dict):
        self.id = node_dict['id']
        self.name = node_dict['name']
        self.started_on = node_dict['startedOn']
        listener = reduce(lambda acc, l: l if l['protocol'] == 'ONetworkProtocolBinary' else acc, node_dict['listeners'])
        if listener:
            listen = listener['listen'].split(':')
            self.host = listen[0]
            self.port = listen[1]

    def __str__(self):
        return self.name
