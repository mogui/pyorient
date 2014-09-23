__author__ = 'Ostico <ostico@gmail.com>'

from pyorient.Messages.Database._TXCommitMessage import _TXCommitMessage


#
# Facade pattern for Commit Message
#
class TxCommitMessage:

    def __init__(self, _orient_socket):
        self._transaction = _TXCommitMessage(_orient_socket)
        pass

    def attach(self, operation):
        self._transaction.attach( operation )
        return self

    def begin(self):
        self._transaction.begin()
        return self

    def commit(self):
        return self._transaction.commit()

    def rollback(self):
        return self._transaction.rollback()