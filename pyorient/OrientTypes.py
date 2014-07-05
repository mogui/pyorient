class OrientRecord(object):
    """
    Object that represent an Orient Document / Record

    """
    __rid = None        
    __version = None    
    __o_class = None

    rid = property(lambda self: self.__rid)
    version = property(lambda self: self.__version)
    o_class = property(lambda self: self.__o_class)

    def __init__(self, content={}, **kwargs):
        """docstring for __init__"""

        for key in content.keys():
            setattr(self, key, content[key])

        self.__rid = kwargs.get('rid', None)
        self.__version = kwargs.get('version', None)
        self.__o_class = kwargs.get('o_class', None)

    def __str__(self):
        return self.__o_class + str(
            [x for x in self.__dict__ if not x.startswith('_Orient')])


class OrientRecordLink(object):
    def __init__(self, recordlink):
        cid, rpos = recordlink.split(":")
        self.__link = recordlink
        self.clusterID = cid
        self.recordPosition = rpos

    def __str__(self):
        return self.getHash()

    def get(self):
        return self.__link

    def getHash(self):
        return "#%s" % self.__link


class OrientBinaryObject(object):
    def __init__(self, stri):
        self.b64 = stri

    def getRaw(self):
        return "_" + self.b64 + "_"

    def getBin(self):
        return base64.b64decode(self.b64)