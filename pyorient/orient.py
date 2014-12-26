# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = 'Ostico <ostico@gmail.com>'

import socket
import struct
import select

from .exceptions import PyOrientBadMethodCallException, \
    PyOrientConnectionException, PyOrientWrongProtocolVersionException

from .constants import FIELD_SHORT, \
    QUERY_ASYNC, QUERY_CMD, QUERY_SYNC, QUERY_SCRIPT, \
    SERIALIZATION_DOCUMENT2CSV, SUPPORTED_PROTOCOL
from .utils import dlog

class OrientSocket(object):
    """docstring for OrientSocket"""

    def __init__(self, host, port):

        self._connected = False
        self.host = host
        self.port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        """:type : socket.socket"""
        self.protocol = -1
        self.session_id = -1
        self.token = ''
        self.db_opened = None
        self.serialization_type = SERIALIZATION_DOCUMENT2CSV
        self.in_transaction = False

    def get_connection(self):
        if not self._connected:
            self.connect()

        return self._socket

    def connect(self):
        dlog("Trying to connect...")
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.settimeout(30)  # 30 secs of timeout
            self._socket.connect( (self.host, self.port) )
            _value = self._socket.recv( FIELD_SHORT['bytes'] )
            self.protocol = struct.unpack('!h', _value)[0]
            if self.protocol > SUPPORTED_PROTOCOL:
                raise PyOrientWrongProtocolVersionException(
                    "Protocol version " + str(self.protocol) +
                    " is not supported yet by this client.", [])
            self._connected = True
        except socket.error as e:
            self._connected = False
            raise PyOrientConnectionException( "Socket Error: %s" % e, [] )

    def close(self):
        self.host = ''
        self.port = ''
        self.protocol = -1
        self.session_id = -1
        self._socket.close()
        self._connected = False

    def write(self, buff):
        return self._socket.send(buff)

    # The man page for recv says: The receive calls normally return
    #   any data available, up to the requested amount, rather than waiting
    #   for receipt of the full amount requested.
    #
    # If you need to read a given number of bytes, you need to call recv
    #   in a loop and concatenate the returned packets until
    #   you have read enough.
    def read(self, _len_to_read):

        while True:

            # This is a trick to detect server disconnection
            # or broken line issues because of
            """:see: https://docs.python.org/2/howto/sockets.html#when-sockets-die """
            try:
                ready_to_read, ready_to_write, in_error = \
                    select.select( [self._socket, ], [self._socket, ], [], 30 )
            except select.error as e:
                self._connected = False
                raise e

            if len(ready_to_read) > 0:

                buf = bytearray(_len_to_read)
                view = memoryview(buf)
                while _len_to_read:
                    n_bytes = self._socket.recv_into(view, _len_to_read)
                    if not n_bytes:
                        self._socket.close()
                        # Additional cleanup
                        raise PyOrientConnectionException(
                            "Server seems to have went down", [])

                    view = view[n_bytes:]  # slicing views is cheap
                    _len_to_read -= n_bytes
                return bytes(buf)

            if len(ready_to_write) > 0:
                # nothing to send
                pass


def ByteToHex( byte_str ):
    """
    Convert a byte string to it's hex string representation e.g. for output.
    """

    # Uses list comprehension which is a fractionally faster implementation than
    # the alternative, more readable, implementation below
    #
    #    hex = []
    #    for aChar in byteStr:
    #        hex.append( "%02X " % ord( aChar ) )
    #
    #    return ''.join( hex ).strip()

    return ''.join( [ "%02X " % ord( x ) for x in byte_str ] ).strip()


#
# OrientDB Message Factory
#
class OrientDB(object):
    _connection = None

    _Messages = dict(
        # Server
        ConnectMessage="pyorient.messages.connection",
        ShutdownMessage="pyorient.messages.connection",

        DbOpenMessage="pyorient.messages.database",
        DbCloseMessage="pyorient.messages.database",
        DbExistsMessage="pyorient.messages.database",
        DbCreateMessage="pyorient.messages.database",
        DbDropMessage="pyorient.messages.database",
        DbCountRecordsMessage="pyorient.messages.database",
        DbReloadMessage="pyorient.messages.database",
        DbSizeMessage="pyorient.messages.database",
        DbListMessage="pyorient.messages.database",

        # Cluster
        DataClusterAddMessage="pyorient.messages.cluster",
        DataClusterCountMessage="pyorient.messages.cluster",
        DataClusterDataRangeMessage="pyorient.messages.cluster",
        DataClusterDropMessage="pyorient.messages.cluster",

        RecordCreateMessage="pyorient.messages.records",
        RecordDeleteMessage="pyorient.messages.records",
        RecordLoadMessage="pyorient.messages.records",
        RecordUpdateMessage="pyorient.messages.records",

        CommandMessage="pyorient.messages.commands",
        TxCommitMessage="pyorient.messages.commands",
    )

    def __init__(self, host='localhost', port=2424):

        if not isinstance(host, OrientSocket):
            connection = OrientSocket(host, port)
        else:
            connection = host

        self._connection = connection

    def __getattr__(self, item):

        _names = "".join( [i.capitalize() for i in item.split('_')] )
        _Message = self.get_message(_names + "Message")

        def wrapper(*args, **kw):
            return _Message.prepare( args ).send().fetch_response()
        return wrapper

    # SERVER COMMANDS

    def connect(self, *args):
        return self.get_message("ConnectMessage") \
            .prepare(args).send().fetch_response()

    def db_count_records(self, *args):
        return self.get_message("DbCountRecordsMessage") \
            .prepare(args).send().fetch_response()

    def db_create(self, *args):
        return self.get_message("DbCreateMessage") \
            .prepare(args).send().fetch_response()

    def db_drop(self, *args):
        return self.get_message("DbDropMessage") \
            .prepare(args).send().fetch_response()

    def db_exists(self, *args):
        return self.get_message("DbExistsMessage") \
            .prepare(args).send().fetch_response()

    def db_open(self, *args):
        return self.get_message("DbOpenMessage") \
            .prepare(args).send().fetch_response()

    def db_reload(self, *args):
        return self.get_message("DbReloadMessage") \
            .prepare(args).send().fetch_response()

    def shutdown(self, *args):
        return self.get_message("ShutdownMessage") \
            .prepare(args).send().fetch_response()

    #DATABASE COMMANDS

    def command(self, *args):
        return self.get_message("CommandMessage") \
            .prepare(( QUERY_CMD, ) + args).send().fetch_response()

    def batch(self, *args):
        return self.get_message("CommandMessage") \
            .prepare(( QUERY_SCRIPT, ) + args).send().fetch_response()

    def query(self, *args):
        return self.get_message("CommandMessage") \
            .prepare(( QUERY_SYNC, ) + args).send().fetch_response()

    def query_async(self, *args):
        return self.get_message("CommandMessage") \
            .prepare(( QUERY_ASYNC, ) + args).send().fetch_response()

    def data_cluster_add(self, *args):
        return self.get_message("DataClusterAddMessage") \
            .prepare(args).send().fetch_response()

    def data_cluster_count(self, *args):
        return self.get_message("DataClusterCountMessage") \
            .prepare(args).send().fetch_response()

    def data_cluster_data_range(self, *args):
        return self.get_message("DataClusterDataRangeMessage") \
            .prepare(args).send().fetch_response()

    def data_cluster_drop(self, *args):
        return self.get_message("DataClusterDropMessage") \
            .prepare(args).send().fetch_response()

    def db_close(self, *args):
        return self.get_message("DbCloseMessage") \
            .prepare(args).send().fetch_response()

    def db_size(self, *args):
        return self.get_message("DbSizeMessage") \
            .prepare(args).send().fetch_response()

    def db_list(self, *args):
        return self.get_message("DbListMessage") \
            .prepare(args).send().fetch_response()

    def record_create(self, *args):
        return self.get_message("RecordCreateMessage") \
            .prepare(args).send().fetch_response()

    def record_delete(self, *args):
        return self.get_message("RecordDeleteMessage") \
            .prepare(args).send().fetch_response()

    def record_load(self, *args):
        return self.get_message("RecordLoadMessage") \
            .prepare(args).send().fetch_response()

    def record_update(self, *args):
        return self.get_message("RecordUpdateMessage") \
            .prepare(args).send().fetch_response()

    def tx_commit(self):
        return self.get_message("TxCommitMessage")

    def get_message(self, command=None):
        """
        Message Factory
        :rtype : pyorient.messages.ConnectMessage,
                 pyorient.messages.DbOpenMessage,
                 pyorient.messages.DbExistsMessage,
                 pyorient.messages.DbCreateMessage,
                 pyorient.messages.DbDropMessage,
                 pyorient.messages.DbCountRecordsMessage,
                 pyorient.messages.DbReloadMessage,
                 pyorient.messages.ShutdownMessage,
                 pyorient.messages.DataClusterAddMessage,
                 pyorient.messages.DataClusterCountMessage,
                 pyorient.messages.DataClusterDataRangeMessage,
                 pyorient.messages.DataClusterDropMessage,
                 pyorient.messages.DbCloseMessage,
                 pyorient.messages.DbSizeMessage,
                 pyorient.messages.DbListMessage,
                 pyorient.messages.RecordCreateMessage,
                 pyorient.messages.RecordDeleteMessage,
                 pyorient.messages.RecordLoadMessage,
                 pyorient.messages.RecordUpdateMessage,
                 pyorient.messages.CommandMessage,
                 pyorient.messages.TXCommitMessage,
        :param command: str
        """
        try:
            if command is not None and self._Messages[command]:
                _msg = __import__(
                    self._Messages[command],
                    globals(),
                    locals(),
                    [command]
                )

                # Get the right instance from Import List
                _Message = getattr(_msg, command)
                return _Message(self._connection)
        except KeyError as e:
            raise PyOrientBadMethodCallException(
                "Unable to find command " + str(e), []
            )