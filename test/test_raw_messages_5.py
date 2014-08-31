__author__ = 'Ostico <ostico@gmail.com>'

import sys
import os
import unittest
from pprint import PrettyPrinter

os.environ['DEBUG'] = "1"
os.environ['DEBUG_VERBOSE'] = "0"
if os.path.realpath( '../' ) not in sys.path:
    sys.path.insert( 0, os.path.realpath( '../' ) )

if os.path.realpath( '.' ) not in sys.path:
    sys.path.insert( 0, os.path.realpath( '.' ) )

from pyorient.Commons.utils import *
from pyorient.Messages.Constants.OrientPrimitives import *
from pyorient.Commons.OrientException import *
from pyorient.Messages.OrientSocket import OrientSocket
from pyorient.Messages.Server.ConnectMessage import ConnectMessage
from pyorient.Messages.Server.DbExistsMessage import DbExistsMessage
from pyorient.Messages.Server.DbOpenMessage import DbOpenMessage
from pyorient.Messages.Server.DbCreateMessage import DbCreateMessage
from pyorient.Messages.Server.DbDropMessage import DbDropMessage
from pyorient.Messages.Server.DbCountRecordsMessage import DbCountRecordsMessage

from pyorient.Messages.Database.CommandMessage import CommandMessage
from pyorient.Messages.Database.RecordLoadMessage import RecordLoadMessage
from pyorient.Messages.Database.RecordCreateMessage import RecordCreateMessage
from pyorient.Messages.Database.RecordUpdateMessage import RecordUpdateMessage
from pyorient.Messages.Database.RecordDeleteMessage import RecordDeleteMessage
from pyorient.Messages.Database.DataClusterCountMessage import DataClusterCountMessage
from pyorient.Messages.Database.DataClusterDataRangeMessage import DataClusterDataRangeMessage
from pyorient.Messages.Database.TxCommitMessage import TXCommitMessage
from pyorient.Commons.OrientTypes import *


class RawMessages_5_TestCase(unittest.TestCase):
    """ Command Test Case """

    def test_attach_class_hint(self):
        try:
            connection = OrientSocket( "localhost", 2424 )
            tx = TXCommitMessage(connection)
            tx.begin()
            tx.attach([1, 2, 3])
            assert False  # should not happens
        except AssertionError, e:
            assert 'A subclass of BaseMessage was expected' == e.message
            assert True

    def test_private_prepare(self):
        try:
            connection = OrientSocket( "localhost", 2424 )
            DbOpenMessage( connection )\
                .prepare(
                    ("GratefulDeadConcerts", "admin", "admin", DB_TYPE_DOCUMENT, "")
                ).send().fetch_response()
    
            tx = TXCommitMessage(connection)
            tx.begin()
            tx.prepare()
            assert False
        except AttributeError, e:
            print e.message
            assert True
    
    
    def test_private_send(self):
        try:
            connection = OrientSocket( "localhost", 2424 )
            DbOpenMessage( connection )\
                .prepare(
                    ("GratefulDeadConcerts", "admin", "admin", DB_TYPE_DOCUMENT, "")
                ).send().fetch_response()
            tx = TXCommitMessage(connection)
            tx.begin()
            tx.send()
            assert False
        except AttributeError, e:
            print e.message
            assert True
    
    def test_private_fetch(self):
        try:
            connection = OrientSocket( "localhost", 2424 )
            DbOpenMessage( connection )\
                .prepare(
                    ("GratefulDeadConcerts", "admin", "admin", DB_TYPE_DOCUMENT, "")
                ).send().fetch_response()
            tx = TXCommitMessage(connection)
            tx.begin()
            tx.fetch_response()
            assert False
        except AttributeError, e:
            print e.message
            assert True

    def test_transaction(self):
        connection = OrientSocket( "localhost", 2424 )
        session_id = ( ConnectMessage( connection ) ).prepare( ("admin", "admin") )\
            .send().fetch_response()

        db_name = "my_little_test"

        msg = DbExistsMessage( connection )
        exists = msg.prepare( [db_name] ).send().fetch_response()

        print "Before %r" % exists
        try:
            ( DbDropMessage( connection ) ).prepare([db_name]) \
                .send().fetch_response()
            assert True
        except PyOrientCommandException, e:
            print e.message
        finally:
            ( DbCreateMessage( connection ) ).prepare(
                (db_name, DB_TYPE_GRAPH, STORAGE_TYPE_PLOCAL)
            ).send().fetch_response()

        msg = DbOpenMessage( connection )
        cluster_info = msg.prepare(
            (db_name, "admin", "admin", DB_TYPE_GRAPH, "")
        ).send().fetch_response()

        try:
            create_class = CommandMessage(connection)
            create_class.prepare((QUERY_CMD, "create class my_class extends V"))\
                .send().fetch_response()
        except PyOrientCommandException, e:
            # class my_class already exists
            # print e
            pass

        # ##################

        # execute real create
        rec = { 'alloggio': 'baita', 'lavoro': 'no', 'vacanza': 'lago' }
        rec_position = ( RecordCreateMessage(connection) )\
            .prepare( ( 3, rec ) )\
            .send().fetch_response()

        # prepare for an update
        rec3 = { 'alloggio': 'albergo', 'lavoro': 'ufficio', 'vacanza': 'montagna' }
        update_success = ( RecordUpdateMessage(connection) )\
            .prepare( ( 3, rec_position.rid, rec3, rec_position.version ) )


        # prepare transaction
        rec1 = { 'alloggio': 'casa', 'lavoro': 'ufficio', 'vacanza': 'mare' }
        rec_position1 = ( RecordCreateMessage(connection) )\
            .prepare( ( -1, rec1 ) )

        rec2 = { 'alloggio': 'baita', 'lavoro': 'no', 'vacanza': 'lago' }
        rec_position2 = ( RecordCreateMessage(connection) )\
            .prepare( ( -1, rec2 ) )


        # create another real record
        rec = { 'alloggio': 'baita', 'lavoro': 'no', 'vacanza': 'lago' }
        rec_position = ( RecordCreateMessage(connection) )\
            .prepare( ( 3, rec ) )\
            .send().fetch_response()

        delete_msg = RecordDeleteMessage(connection)
        delete_msg.prepare( ( 3, rec_position.rid ) )


        tx = TXCommitMessage(connection)
        tx.begin()
        tx.attach( rec_position1 )
        tx.attach( rec_position1 )
        tx.attach( rec_position2 )
        tx.attach( update_success )
        tx.attach( delete_msg )
        res = tx.commit()

        for k, v in res.iteritems():
            print k + " -> " + v.vacanza

        assert len(res) == 4
        assert res["#3:0"].vacanza == 'montagna'
        assert res["#3:2"].vacanza == 'mare'
        assert res["#3:3"].vacanza == 'mare'
        assert res["#3:4"].vacanza == 'lago'

        # print ""
        # # at the end drop the test database
        ( DbDropMessage( connection ) ).prepare([db_name, STORAGE_TYPE_MEMORY]) \
            .send().fetch_response()

# test_private_prepare()
# test_private_send()
# test_private_fetch()
# test_transaction()