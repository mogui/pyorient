__author__ = 'Ostico <ostico@gmail.com>'
from .Commons.OrientException import *
from .Messages.OrientSocket import OrientSocket


#
# OrientDB Message Factory
#
class OrientDB():
    _connection = None

    _Messages = dict(
        # Server
        ConnectMessage="pyorient.Messages.Server.ConnectMessage",
        DbOpenMessage="pyorient.Messages.Server.DbOpenMessage",
        DbExistsMessage="pyorient.Messages.Server.DbExistsMessage",
        DbCreateMessage="pyorient.Messages.Server.DbCreateMessage",
        DbDropMessage="pyorient.Messages.Server.DbDropMessage",
        DbCountRecordsMessage="pyorient.Messages.Server.DbCountRecordsMessage",
        DbReloadMessage="pyorient.Messages.Server.DbReloadMessage",
        ShutdownMessage="pyorient.Messages.Server.ShutdownMessage",

        # Database
        DataClusterAddMessage="pyorient.Messages.Database.DataClusterAddMessage",
        DataClusterCountMessage="pyorient.Messages.Database.DataClusterCountMessage",
        DataClusterDataRangeMessage="pyorient.Messages.Database.DataClusterDataRangeMessage",
        DataClusterDropMessage="pyorient.Messages.Database.DataClusterDropMessage",
        DbCloseMessage="pyorient.Messages.Database.DbCloseMessage",
        DbSizeMessage="pyorient.Messages.Database.DbSizeMessage",
        RecordCreateMessage="pyorient.Messages.Database.RecordCreateMessage",
        RecordDeleteMessage="pyorient.Messages.Database.RecordDeleteMessage",
        RecordLoadMessage="pyorient.Messages.Database.RecordLoadMessage",
        RecordUpdateMessage="pyorient.Messages.Database.RecordUpdateMessage",
        CommandMessage="pyorient.Messages.Database.CommandMessage",
        TxCommitMessage="pyorient.Messages.Database.TxCommitMessage",
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
            .prepare(( pyorient.QUERY_CMD, ) + args).send().fetch_response()

    def query(self, *args):
        return self.get_message("CommandMessage") \
            .prepare(( pyorient.QUERY_SYNC, ) + args).send().fetch_response()

    def query_async(self, *args):
        return self.get_message("CommandMessage") \
            .prepare(( pyorient.QUERY_ASYNC, ) + args).send().fetch_response()

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
        :rtype : pyorient.Messages.Server.ConnectMessage,
                 pyorient.Messages.Server.DbOpenMessage,
                 pyorient.Messages.Server.DbExistsMessage,
                 pyorient.Messages.Server.DbCreateMessage,
                 pyorient.Messages.Server.DbDropMessage,
                 pyorient.Messages.Server.DbCountRecordsMessage,
                 pyorient.Messages.Server.DbReloadMessage,
                 pyorient.Messages.Server.ShutdownMessage,
                 pyorient.Messages.Database.DataClusterAddMessage,
                 pyorient.Messages.Database.DataClusterCountMessage,
                 pyorient.Messages.Database.DataClusterDataRangeMessage,
                 pyorient.Messages.Database.DataClusterDropMessage,
                 pyorient.Messages.Database.DbCloseMessage,
                 pyorient.Messages.Database.DbSizeMessage,
                 pyorient.Messages.Database.RecordCreateMessage,
                 pyorient.Messages.Database.RecordDeleteMessage,
                 pyorient.Messages.Database.RecordLoadMessage,
                 pyorient.Messages.Database.RecordUpdateMessage,
                 pyorient.Messages.Database.CommandMessage,
                 pyorient.Messages.Database.TXCommitMessage,
        :param command: str
        """
        try:
            if command is not None and self._Messages[command]:
                _msg = __import__(
                    self._Messages[command],
                    globals(),
                    locals(),
                    [command],
                    -1
                )

                # Get the right instance from Import List
                _Message = getattr(_msg, command)
                return _Message(self._connection)
        except KeyError, e:
            raise PyOrientBadMethodCallException(
                "Unable to find command " + e.message, []
            )