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