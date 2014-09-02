import json
import re
import _pyorient

from ORecordCoder import ORecordDecoder, ORecordEncoder
from OrientTypes import OrientRecordLink, OrientRecord, OrientBinaryObject
from OrientException import PyOrientException


class OrientDB(object):
    """This Class Wrap the c module any method not overrided behaves like the c binding"""

    def __init__(self, host, port, user, pwd):
        self._connection_ref = int(_pyorient.connect(host, port, user, pwd))
        if(self._connection_ref < 0):
            raise _pyorient.PyOrientModuleException("Not connected to DB")

    def __getattr__(self, name):
        """ This does the magic wrap :) """

        def function(*args, **kwargs):
            # adding the connection reference
            kwargs['conn'] = self._connection_ref

            try:
                asd = getattr(_pyorient, name)
                return asd(*args, **kwargs)
            except _pyorient.PyOrientModuleException, e:
                raise PyOrientException("catched %s" % e)

        return function

    def command(self, query, limit=20, fetchplan="*:-1", async=False, **kwargs):
        """docstring for command"""

        if async:
            kwargs['command_type'] = QUERY_ASYNC

        raw_result = _pyorient.command(query, limit, **kwargs)

        if kwargs.get('raw', False):
            return raw_result

        ret = []

        for raw_record in raw_result:
            parser = ORecordDecoder(raw_record)
            record = OrientRecord(parser.data, o_class=parser.className)
            ret.append(record)

        return ret

    def recordload(self, cluster_id, cluster_position, **kwargs):
        raw_record = _pyorient.recordload(
            cluster_id, cluster_position, **kwargs)
        if kwargs.get('raw_result', False):
            return raw_record

        parser = ORecordDecoder(raw_record)

        record = OrientRecord(
            parser.data, o_class=parser.className,
            rid="#%d:%d" % (cluster_id, cluster_position))
        # @TODO missing rid and version from c api)
        return record

    def recordcreate(self, cluster_id, record, **kwargs):
        if not isinstance(record, OrientRecord):
            record = OrientRecord(record)
        parser = ORecordEncoder(record)
        raw_record = str(parser.getRaw().encode('utf-8'))
        ret = _pyorient.recordcreate(cluster_id, raw_record, **kwargs)

        return ret
