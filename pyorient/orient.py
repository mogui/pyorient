# -*- coding: utf-8 -*-
"""
@author: Ostico <ostico@gmail.com>
"""
from __future__ import print_function

__author__ = 'Ostico <ostico@gmail.com>'

import socket
import struct
import select

from .exceptions import PyOrientBadMethodCallException, \
    PyOrientConnectionException, PyOrientWrongProtocolVersionException, \
    PyOrientConnectionPoolException

from .constants import FIELD_SHORT, \
    QUERY_ASYNC, QUERY_CMD, QUERY_GREMLIN, QUERY_SYNC, QUERY_SCRIPT, \
    SUPPORTED_PROTOCOL, DB_TYPE_DOCUMENT, \
    STORAGE_TYPE_PLOCAL, SOCK_CONN_TIMEOUT

from .serializations import OrientSerialization

from .utils import dlog

class OrientSocket(object):
    '''Class representing the binary connection to the database, it does all the low level comunication
    And holds information on server version and cluster map

    .. DANGER::
      Should not be used directly

    :param host: hostname of the server to connect
    :param port: integer port of the server

    '''
    def __init__(self, host, port):

        self.connected = False
        self.host = host
        self.port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.protocol = -1
        self.session_id = -1
        self.auth_token = b''
        self.db_opened = None
        self.serialization_type = OrientSerialization.CSV
        self.in_transaction = False

    def get_connection(self):
        if not self.connected:
            self.connect()

        return self._socket

    def connect(self):
        '''Connects to the inner socket
        could raise :class:`PyOrientConnectionPoolException`
        '''
        dlog("Trying to connect...")
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.settimeout( SOCK_CONN_TIMEOUT )  # 30 secs of timeout
            self._socket.connect( (self.host, self.port) )
            _value = self._socket.recv( FIELD_SHORT['bytes'] )

            if len(_value) != 2:
                self._socket.close()

                raise PyOrientConnectionPoolException(
                    "Server sent empty string", []
                )

            self.protocol = struct.unpack('!h', _value)[0]
#            if self.protocol > SUPPORTED_PROTOCOL:
#                raise PyOrientWrongProtocolVersionException(
#                    "Protocol version " + str(self.protocol) +
#                    " is not supported yet by this client.", [])
            self.connected = True
        except socket.error as e:
            self.connected = False
            raise PyOrientConnectionException( "Socket Error: %s" % e, [] )

    def close(self):
        '''Close the inner connection
        '''
        self.host = ''
        self.port = 0
        self.protocol = -1
        self.session_id = -1
        self._socket.close()
        self.connected = False

    def write(self, buff):
        # This is a trick to detect server disconnection
        # or broken line issues because of
        """:see: https://docs.python.org/2/howto/sockets.html#when-sockets-die """

        try:
            _, ready_to_write, in_error = select.select(
                [], [self._socket], [self._socket], 1)
        except select.error as e:
            self.connected = False
            self._socket.close()
            raise e

        if not in_error and ready_to_write:
            self._socket.sendall(buff)
            return len(buff)
        else:
            self.connected = False
            self._socket.close()
            raise PyOrientConnectionException("Socket error", [])

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
                ready_to_read, _, in_error = \
                    select.select( [self._socket, ], [], [self._socket, ], 30 )
            except select.error as e:
                self.connected = False
                self._socket.close()
                raise e

            if len(ready_to_read) > 0:

                buf = bytearray(_len_to_read)
                view = memoryview(buf)
                while _len_to_read:
                    n_bytes = self._socket.recv_into(view, _len_to_read)
                    if not n_bytes:
                        self._socket.close()
                        # TODO Implement re-connection to another listener

                        raise PyOrientConnectionException(
                            "Server seems to have went down", [])

                    view = view[n_bytes:]  # slicing views is cheap
                    _len_to_read -= n_bytes
                return bytes(buf)

            if len(in_error) > 0:
                self._socket.close()
                raise PyOrientConnectionException(
                    "Socket error", [])



class OrientDB(object):
    """OrientDB client object

    Point of entrance to use the basic commands you can issue to the server

    :param host: hostname of the server to connect  defaults to localhost
    :param port: integer port of the server         defaults to 2424

    Usage::

        >>> from pyorient import OrientDB
        >>> client = OrientDB("localhost", 2424)
        >>> client.db_open('MyDatabase', 'admin', 'admin')


    """
    _connection  = None
    _auth_token  = None

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

        #: an :class:`OrientVersion <OrientVersion>` object representing connected server version, None if
        #: not connected
        self.version = None

        #: array of :class:`OrientCluser <OrientCluser>` representing the connected database clusters
        self.clusters = []

        #: array of :class:`OrientNode <OrientNode>` if the connected server is in a distributed cluster config
        self.nodes = []

        self._cluster_map = None
        self._cluster_reverse_map = None
        self._connection = connection

    def __getattr__(self, item):

        _names = "".join( [i.capitalize() for i in item.split('_')] )
        _Message = self.get_message(_names + "Message")

        def wrapper(*args, **kw):
            return _Message.prepare( args ).send().fetch_response()
        return wrapper

    def _reload_clusters(self):
        self._cluster_map = dict([(cluster.name, cluster.id) for cluster in self.clusters])
        self._cluster_reverse_map = dict([(cluster.id, cluster.name) for cluster in self.clusters])

    def get_class_position(self, cluster_name):
        """
        Get the cluster position (id) given a cluster name
        :param cluster_name: cluster name
        :return: int cluster id
        """
        return self._cluster_map[cluster_name.lower()]

    def get_class_name(self, position):
        """
        Get cluster name given a cluster position (id)
        :param position: cluster id
        :return: string cluster name
        """
        return self._cluster_reverse_map[position]

    def set_session_token( self, token ):
        """
        Set true if you want to use token authentication
        :param token: bool
        """
        self._auth_token = token
        return self

    def get_session_token( self ):
        """Returns the auth token of the session
        """
        return self._connection.auth_token




    # SERVER COMMANDS

    def connect(self, user, password, client_id='', serialization_type=OrientSerialization.CSV):
        '''Connect to the server without opening any database

        :param user: the username of the user on the server. Example: "root"
        :param password: the password of the user on the server. Example: "37aed6392"
        :param client_id: client's id - can be null for clients. In clustered configurations it's the distributed node ID as TCP host:port
        :param serialization_type: the serialization format required by the client, now it can be just OrientSerialization.CSV

        Usage to open a connection as root::

            >>> from pyorient import OrientDB
            >>> client = OrientDB("localhost", 2424)
            >>> client.connect('root', 'root')

        '''
        return self.get_message("ConnectMessage") \
            .prepare((user, password, client_id, serialization_type)).send().fetch_response()

    def db_count_records(self):
        '''Returns the number of records in the currently open database.

        :return: long

        Usage::

            >>> from pyorient import OrientDB
            >>> client = OrientDB("localhost", 2424)
            >>> client.db_open('MyDatabase', 'admin', 'admin')
            >>> client.db_count_records()
            7872
        '''
        return self.get_message("DbCountRecordsMessage") \
            .prepare(()).send().fetch_response()

    def db_create(self, name, type=DB_TYPE_DOCUMENT, storage=STORAGE_TYPE_PLOCAL):
        '''Creates a database in the remote OrientDB server instance.

        :param name: the name of the database to create. Example: "MyDatabase".
        :param type: the type of the database to create. Can be either document or graph. [default: DB_TYPE_DOCUMENT]
        :param storage:  specifies the storage type of the database to create. It can be one of the supported types [default: STORAGE_TYPE_PLOCAL]:

            - STORAGE_TYPE_PLOCAL - persistent database
            - STORAGE_TYPE_MEMORY - volatile database

        :return: None

        Usage::

            >>> from pyorient import OrientDB
            >>> client = OrientDB("localhost", 2424)
            >>> client.connect('root', 'root')
            >>> client.db_create('test')
        '''
        self.get_message("DbCreateMessage") \
            .prepare((name, type, storage)).send().fetch_response()
        return None

    def db_drop(self, name, type=STORAGE_TYPE_PLOCAL):
        '''Removes a database from the OrientDB server instance. This operation returns a successful response if the database is deleted successfully. Otherwise, if the database doesn't exist on the server, it returns an Exception

        :param name: the name of the database to create. Example: "MyDatabase".
        :param type: the type of the database to create. Can be either document or graph. [default: DB_TYPE_DOCUMENT]

        :return: None
        '''
        self.get_message("DbDropMessage") \
            .prepare((name, type)).send().fetch_response()
        return None

    def db_exists(self, name, type=STORAGE_TYPE_PLOCAL):
        '''Asks if a database exists in the OrientDB server instance.

        :param name: the name of the database to create. Example: "MyDatabase".
        :param type: the type of the database to create. Can be either document or graph. [default: DB_TYPE_DOCUMENT]

        :return: bool
        '''

        return self.get_message("DbExistsMessage") \
            .prepare((name, type)).send().fetch_response()

    def db_open(self, db_name, user, password, db_type=DB_TYPE_DOCUMENT, client_id=''):
        '''
         Opens a database on the remote OrientDB Server.
         Returns the Session-Id to being reused for all the next calls and the list of configured clusters

        :param db_name: database name as string. Example: "demo"
        :param user: username as string
        :param password: password as string
        :param db_type: string, can be DB_TYPE_DOCUMENT or DB_TYPE_GRAPH
        :param client_id: Can be null for clients. In clustered configuration is the distributed node
        :return: an array of :class:`OrientCluster <pyorient.types.OrientCluster>` object

        Usage::

          >>> import pyorient
          >>> orient = pyorient.OrientDB('localhost', 2424)
          >>> orient.db_open('asd', 'admin', 'admin')

        '''

        info, clusters, nodes = self.get_message("DbOpenMessage") \
            .prepare((db_name, user, password, db_type, client_id)).send().fetch_response()

        self.version = info
        self.clusters = clusters
        self._reload_clusters()
        self.nodes = nodes

        return self.clusters

    def db_reload(self):
        """
        Reloads current connected database

        :return: renewed array of :class:`OrientCluster <pyorient.types.OrientCluster>`
        """
        self.clusters = self.get_message("DbReloadMessage") \
            .prepare([]).send().fetch_response()
        self._reload_clusters()
        return self.clusters

    def shutdown(self, *args):
        return self.get_message("ShutdownMessage") \
            .prepare(args).send().fetch_response()

    # DATABASE COMMANDS

    def gremlin(self, *args):
        return self.get_message("CommandMessage") \
            .prepare(( QUERY_GREMLIN, ) + args).send().fetch_response()

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
                if self._connection.auth_token != b'':
                    token = self._connection.auth_token
                else:
                    token = self._auth_token

                message_instance = _Message(self._connection)\
                    .set_session_token(token)
                message_instance._push_callback = self._push_received
                return message_instance

        except KeyError as e:
            self._connection.close()
            raise PyOrientBadMethodCallException(
                "Unable to find command " + str(e), []
            )

    def _push_received(self, command_id, payload):
        # REQUEST_PUSH_RECORD	        79
        # REQUEST_PUSH_DISTRIB_CONFIG	80
        # REQUEST_PUSH_LIVE_QUERY	    81
        # TODO: this logic must stay within Messages class here I just want to receive
        # an object of something, like a new array of cluster.
        if command_id == 80:
            pass
