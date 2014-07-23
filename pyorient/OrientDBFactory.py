__author__ = 'Ostico <ostico@gmail.com>'
from pyorient.Commons.OrientException import *
from pyorient.Messages.OrientSocket import OrientSocket


#
# OrientDB Message Factory
#
class OrientDBFactory(object):
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
        TXCommitMessage="pyorient.Messages.Database.TXCommitMessage",
    )

    def __init__(self, host='localhost', port=2424):

        if not isinstance(host, OrientSocket):
            connection = OrientSocket(host, port)
        else:
            connection = host

        self._connection = connection

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