# -*- coding: utf-8 -*-
__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.exceptions import PyOrientBadMethodCallException

from .base import BaseMessage
from ..constants import DB_OPEN_OP, DB_TYPE_DOCUMENT, DB_COUNT_RECORDS_OP, FIELD_BYTE, FIELD_INT, \
    FIELD_SHORT, FIELD_STRING, FIELD_STRINGS, FIELD_BYTES, FIELD_BOOLEAN, NAME, SUPPORTED_PROTOCOL, \
    VERSION, DB_TYPES, DB_CLOSE_OP, DB_EXIST_OP, STORAGE_TYPE_PLOCAL, \
    STORAGE_TYPE_LOCAL, DB_CREATE_OP, \
    DB_DROP_OP, DB_RELOAD_OP, DB_SIZE_OP, DB_LIST_OP, STORAGE_TYPES, FIELD_LONG
from ..utils import need_connected, need_db_opened
from ..otypes import OrientRecord, OrientCluster, OrientVersion, OrientNode
from ..serializations import OrientSerialization

#
# DB OPEN
#
# This is the first operation the client should call.
# It opens a database on the remote OrientDB Server.
# Returns the Session-Id to being reused for all the next calls and
# the list of configured clusters.
#
# Request: (driver-name:string)(driver-version:string)
#   (protocol-version:short)(client-id:string)(serialization-impl:string)
#   (database-name:string)(database-type:string)(user-name:string)(user-password:string)
# Response:(session-id:int)(num-of-clusters:short)[(cluster-name:string)
#   (cluster-id:short)](cluster-config:bytes.md)(orientdb-release:string)
#
# client's driver-name as string. Example: "OrientDB Java client"
# client's driver-version as string. Example: "1.0rc8-SNAPSHOT"
# client's protocol-version as short. Example: 7
# client's client-id as string. Can be null for clients. In clustered configuration
#   is the distributed node ID as TCP host+port. Example: "10.10.10.10:2480"
# client's serialization-impl the serialization format required by the client.
# database-name as string. Example: "demo"
# database-type as string, can be 'document' or 'graph' (since version 8). Example: "document"
# user-name as string. Example: "admin"
# user-password as string. Example: "admin"
# cluster-config is always null unless you're running in a server clustered configuration.
# orientdb-release as string. Contains version of OrientDB release
#   deployed on server and optionally build number. Example: "1.4.0-SNAPSHOT (build 13)"
#
#
class DbOpenMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(DbOpenMessage, self).__init__(_orient_socket)

        self._user = ''
        self._pass = ''
        self._client_id = ''
        self._db_name = ''
        self._db_type = DB_TYPE_DOCUMENT
        self._append(( FIELD_BYTE, DB_OPEN_OP ))
        self._need_token = False

    def prepare(self, params=None):

        if isinstance(params, tuple) or isinstance(params, list):
            try:
                self._db_name = params[0]
                self._user = params[1]
                self._pass = params[2]
                self.set_db_type(params[3])
                self._client_id = params[4]
                
            except IndexError:
                # Use default for non existent indexes
                pass

        self._append(( FIELD_STRINGS, [NAME, VERSION] ))
        self._append(( FIELD_SHORT, SUPPORTED_PROTOCOL ))
        self._append(( FIELD_STRING, self._client_id ))


        if self.get_protocol() > 21:
            self._append(( FIELD_STRING, self._orientSocket.serialization_type ))
            if self.get_protocol() > 26:
                self._append(( FIELD_BOOLEAN, self._request_token ))
                if self.get_protocol() >= 36:
                    self._append(( FIELD_BOOLEAN, True ))  # support-push
                    self._append(( FIELD_BOOLEAN, True ))  # collect-stats

        self._append(( FIELD_STRING, self._db_name ))

        if self.get_protocol() < 33:
            self._append(( FIELD_STRING, self._db_type ))

        self._append(( FIELD_STRING, self._user ))
        self._append(( FIELD_STRING, self._pass ))

        return super(DbOpenMessage, self).prepare()

    def fetch_response(self):
        self._append(FIELD_INT)  # session_id
        if self.get_protocol() > 26:
            self._append(FIELD_STRING)  # token # if FALSE: Placeholder

        self._append(FIELD_SHORT)  # cluster_num

        result = super(DbOpenMessage, self).fetch_response()
        if self.get_protocol() > 26:
            self._session_id, self._auth_token, cluster_num = result
            if self._auth_token == b'':
                self.set_session_token(False)
            self._update_socket_token()
        else:
            self._session_id, cluster_num = result

        # IMPORTANT needed to pass the id to other messages
        self._update_socket_id()

        clusters = []

        # Parsing cluster map TODO: this must be put in serialization interface
        for x in range(0, cluster_num):
            if self.get_protocol() < 24:
                cluster = OrientCluster(
                    self._decode_field(FIELD_STRING),
                    self._decode_field(FIELD_SHORT),
                    self._decode_field(FIELD_STRING),
                    self._decode_field(FIELD_SHORT)
                )
            else:
                cluster = OrientCluster(
                    self._decode_field(FIELD_STRING),
                    self._decode_field(FIELD_SHORT)
                )
            clusters.append(cluster)

        self._append(FIELD_STRING)  # orient node list | string ""
        self._append(FIELD_STRING)  # Orient release

        nodes_config, release = super(DbOpenMessage, self).fetch_response(True)

        # parsing server release version
        info = OrientVersion(release)

        nodes = []

        # parsing Node List TODO: this must be put in serialization interface
        if len(nodes_config) > 0:
            _, decoded = self.get_serializer().decode(nodes_config)
            self._node_list = []
            for node_dict in decoded['members']:
                self._node_list.append(OrientNode(node_dict))

        # set database opened
        self._orientSocket.db_opened = self._db_name

        return info, clusters, self._node_list
        # self._cluster_map = self._orientSocket.cluster_map = \
        #     Information([clusters, response, self._orientSocket])

        # return self._cluster_map

    def set_db_name(self, db_name):
        self._db_name = db_name
        return self

    def set_db_type(self, db_type):
        if db_type in DB_TYPES:
            # user choice storage if present
            self._db_type = db_type
        else:
            raise PyOrientBadMethodCallException(
                db_type + ' is not a valid database type', []
            )
        return self

    def set_client_id(self, _cid):
        self._client_id = _cid
        return self

    def set_user(self, _user):
        self._user = _user
        return self

    def set_pass(self, _pass):
        self._pass = _pass
        return self


#
# DB CLOSE
#
# Closes the database and the network connection to the OrientDB Server
# instance. No return is expected. The socket is also closed.
#
# Request: empty
# Response: no response, the socket is just closed at server side
#
class DbCloseMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(DbCloseMessage, self).__init__(_orient_socket)

        # order matters
        self._append(( FIELD_BYTE, DB_CLOSE_OP ))

    @need_connected
    def prepare(self, params=None):
        return super(DbCloseMessage, self).prepare()

    def fetch_response(self):
        # set database closed
        self._orientSocket.db_opened = None
        super(DbCloseMessage, self).close()
        return 0


#
# DB EXISTS
#
# Asks if a database exists in the OrientDB Server instance. It returns true (non-zero) or false (zero).
#
# Request: (database-name:string) <-- before 1.0rc1 this was empty (server-storage-type:string - since 1.5-snapshot)
# Response: (result:byte)
#
# server-storage-type can be one of the supported types:
# plocal as a persistent database
# memory, as a volatile database
#
class DbExistsMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(DbExistsMessage, self).__init__(_orient_socket)

        self._db_name = ''
        self._storage_type = ''

        if self.get_protocol() > 16:  # 1.5-SNAPSHOT
            self._storage_type = STORAGE_TYPE_PLOCAL
        else:
            self._storage_type = STORAGE_TYPE_LOCAL

        # order matters
        self._append(( FIELD_BYTE, DB_EXIST_OP ))

    @need_connected
    def prepare(self, params=None):

        if isinstance(params, tuple) or isinstance(params, list):
            try:
                self._db_name = params[0]
                # user choice storage if present
                self.set_storage_type(params[1])

            except IndexError:
                # Use default for non existent indexes
                pass

        if self.get_protocol() >= 6:
            self._append(( FIELD_STRING, self._db_name ))  # db_name

        if self.get_protocol() >= 16:
            # > 16 1.5-snapshot
            # custom choice server_storage_type
            self._append(( FIELD_STRING, self._storage_type ))

        return super(DbExistsMessage, self).prepare()

    def fetch_response(self):
        self._append(FIELD_BOOLEAN)
        return super(DbExistsMessage, self).fetch_response()[0]

    def set_db_name(self, db_name):
        self._db_name = db_name
        return self

    def set_storage_type(self, storage_type):
        if storage_type in STORAGE_TYPES:
            # user choice storage if present
            self._storage_type = storage_type
        else:
            raise PyOrientBadMethodCallException(
                storage_type + ' is not a valid storage type', []
            )
        return self


#
# DB CREATE
#
# Creates a database in the remote OrientDB server instance
#
# Request: (database-name:string)(database-type:string)(storage-type:string)
# Response: empty
#
# - database-name as string. Example: "demo"
# - database-type as string, can be 'document' or 'graph' (since version 8). Example: "document"
# - storage-type can be one of the supported types:
# - plocal, as a persistent database
# - memory, as a volatile database
#
class DbCreateMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(DbCreateMessage, self).__init__(_orient_socket)

        self._db_name = ''
        self._db_type = DB_TYPE_DOCUMENT
        self._storage_type = ''
        self._backup_path = -1

        if self.get_protocol() > 16:  # 1.5-SNAPSHOT
            self._storage_type = STORAGE_TYPE_PLOCAL
        else:
            self._storage_type = STORAGE_TYPE_LOCAL

        # order matters
        self._append(( FIELD_BYTE, DB_CREATE_OP ))

    @need_connected
    def prepare(self, params=None):

        if isinstance(params, tuple) or isinstance(params, list):
            try:
                self._db_name = params[0]
                self.set_db_type(params[1])
                self.set_storage_type(params[2])
                self.set_backup_path(params[3])
            except IndexError:
                pass

        self._append(
            (FIELD_STRINGS, [self._db_name, self._db_type, self._storage_type ])
        )

        if self.get_protocol() > 35:
            if isinstance( self._backup_path, int ):
                field_type = FIELD_INT
            else:
                field_type = FIELD_STRING
            self._append( ( field_type, self._backup_path ) )

        return super(DbCreateMessage, self).prepare()

    def fetch_response(self):
        super(DbCreateMessage, self).fetch_response()
        # set database opened
        self._orientSocket.db_opened = self._db_name
        return

    def set_db_name(self, db_name):
        self._db_name = db_name
        return self

    def set_backup_path(self, backup_path):
        self._backup_path = backup_path
        return self

    def set_db_type(self, db_type):
        if db_type in DB_TYPES:
            # user choice storage if present
            self._db_type = db_type
        else:
            raise PyOrientBadMethodCallException(
                db_type + ' is not a valid database type', []
            )
        return self

    def set_storage_type(self, storage_type):
        if storage_type in STORAGE_TYPES:
            # user choice storage if present
            self._storage_type = storage_type
        else:
            raise PyOrientBadMethodCallException(
                storage_type + ' is not a valid storage type', []
            )
        return self


#
# DB DROP
#
# Removes a database from the OrientDB Server instance.
# It returns nothing if the database has been deleted or throws
# a OStorageException if the database doesn't exists.
#
# Request: (database-name:string)(server-storage-type:string - since 1.5-snapshot)
# Response: empty
#
# - server-storage-type can be one of the supported types:
# - plocal as a persistent database
# - memory, as a volatile database
#
class DbDropMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(DbDropMessage, self). \
            __init__(_orient_socket)

        self._db_name = ''
        self._storage_type = ''

        if self.get_protocol() > 16:  # 1.5-SNAPSHOT
            self._storage_type = STORAGE_TYPE_PLOCAL
        else:
            self._storage_type = STORAGE_TYPE_LOCAL

        # order matters
        self._append(( FIELD_BYTE, DB_DROP_OP ))

    @need_connected
    def prepare(self, params=None):

        if isinstance(params, tuple) or isinstance(params, list):
            try:
                self._db_name = params[0]
                self.set_storage_type(params[1])
            except IndexError:
                # Use default for non existent indexes
                pass

        self._append(( FIELD_STRING, self._db_name ))  # db_name

        if self.get_protocol() >= 16:  # > 16 1.5-snapshot
            # custom choice server_storage_type
            self._append(( FIELD_STRING, self._storage_type ))

        return super(DbDropMessage, self).prepare()

    def fetch_response(self):
        return super(DbDropMessage, self).fetch_response()

    def set_db_name(self, db_name):
        self._db_name = db_name
        return self

    def set_storage_type(self, storage_type):
        if storage_type in STORAGE_TYPES:
            # user choice storage if present
            self._storage_type = storage_type
        else:
            raise PyOrientBadMethodCallException(
                storage_type + ' is not a valid storage type', []
            )
        return self


#
# DB COUNT RECORDS
#
# Asks for the number of records in a database in
# the OrientDB Server instance.
#
# Request: empty
# Response: (count:long)
#
class DbCountRecordsMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(DbCountRecordsMessage, self).__init__(_orient_socket)

        self._user = ''
        self._pass = ''

        # order matters
        self._append(( FIELD_BYTE, DB_COUNT_RECORDS_OP ))

    @need_db_opened
    def prepare(self, params=None):
        return super(DbCountRecordsMessage, self).prepare()

    def fetch_response(self):
        self._append(FIELD_LONG)
        return super(DbCountRecordsMessage, self).fetch_response()[0]


#
# DB RELOAD
#
# Reloads database information. Available since 1.0rc4.
# 
# Request: empty
# Response:(num-of-clusters:short)[(cluster-name:string)(cluster-id:short)]
#
class DbReloadMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(DbReloadMessage, self).__init__(_orient_socket)

        # order matters
        self._append(( FIELD_BYTE, DB_RELOAD_OP ))

    @need_connected
    def prepare(self, params=None):
        return super(DbReloadMessage, self).prepare()

    def fetch_response(self):

        self._append(FIELD_SHORT)  # cluster_num

        cluster_num = super(DbReloadMessage, self).fetch_response()[0]

        clusters = []

        # Parsing cluster map
        for x in range(0, cluster_num):
            if self.get_protocol() < 24:
                cluster = OrientCluster(
                    self._decode_field(FIELD_STRING),
                    self._decode_field(FIELD_SHORT),
                    self._decode_field(FIELD_STRING),
                    self._decode_field(FIELD_SHORT)
                )
            else:
                cluster = OrientCluster(
                    self._decode_field(FIELD_STRING),
                    self._decode_field(FIELD_SHORT)
                )
            clusters.append(cluster)

        return clusters


#
# DB SIZE
#
# Asks for the size of a database in the OrientDB Server instance.
#
# Request: empty
# Response: (size:long)
#
class DbSizeMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(DbSizeMessage, self).__init__(_orient_socket)

        # order matters
        self._append(( FIELD_BYTE, DB_SIZE_OP ))

    @need_db_opened
    def prepare(self, params=None):
        return super(DbSizeMessage, self).prepare()

    def fetch_response(self):
        self._append(FIELD_LONG)
        return super(DbSizeMessage, self).fetch_response()[0]


#
# DB List
#
# Asks for the size of a database in the OrientDB Server instance.
#
# Request: empty
# Response: (size:long)
#
class DbListMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super(DbListMessage, self).__init__(_orient_socket)

        # order matters
        self._append(( FIELD_BYTE, DB_LIST_OP ))

    @need_connected
    def prepare(self, params=None):
        return super(DbListMessage, self).prepare()

    def fetch_response(self):
        self._append(FIELD_BYTES)
        __record = super(DbListMessage, self).fetch_response()[0]
        # bug in orientdb csv serialization in snapshot 2.0,
        # strip trailing spaces
        _, data = self.get_serializer().decode(__record.rstrip())

        return OrientRecord(dict(__o_storage=data))
