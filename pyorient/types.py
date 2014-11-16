class OrientRecord(object):
    """
    Object that represent an Orient Document / Record

    """
    __rid = None
    __version = None
    __o_class = None
    __o_storage = {}

    rid = property(lambda self: self.__rid)
    version = property(lambda self: self.__version)
    o_class = property(lambda self: self.__o_class)
    oRecordData = property(lambda self: self.__o_storage)

    def __init__(self, content={}, **kwargs):
        """docstring for __init__"""

        self.__rid = kwargs.get('rid', None)
        self.__o_class = kwargs.get('o_class', None)
        self.__version = kwargs.get('version', None)
        self.__o_storage = {}
        self._set_keys(content)

    def _set_keys(self, content=dict):
        for key in content.keys():
            if key == 'rid':  # Ex: select @rid, field from v_class
                self.__rid = content[key]
            elif key == 'version':  # Ex: select @rid, @version from v_class
                self.__version = content[key]
            elif key[0] != '@':
                self.__o_storage[key] = content[key]
            else:
                self.__o_class = key[1:]
                self._set_keys( content[key] )

    def __str__(self):
        rep = str( self.__o_storage )
        if self.__o_class is not None:
            rep = "{'@" + str(self.__o_class) + "':" + rep + "}"
        return rep

    def update(self, **kwargs):
        self.__rid = kwargs.get('rid', None)
        self.__version = kwargs.get('version', None)
        if self.__o_class is None:
            self.__o_class = kwargs.get('o_class', None)

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

    """ This method is for backward compatibility when someone
        use 'getattr(record, a_key)' """
    def __getattr__(self, item):
        """
        :param item: string
        :return: mixed
        :raise: AttributeError
        """
        if item not in [ 'rid', 'version', 'o_class' ]:
            try:
                return self.__o_storage[item]
            except KeyError:
                raise AttributeError( "'OrientRecord' object has no attribute "
                                      "'" + item + "'" )
        else:
            return self.__getattribute__(item)


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
    def __init__(self, stri):
        self.b64 = stri

    def getRaw(self):
        return "_" + self.b64 + "_"

    def getBin(self):
        import base64
        return base64.b64decode(self.b64)