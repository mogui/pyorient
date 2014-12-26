__author__ = 'Ostico <ostico@gmail.com>'

import sys
import os
import unittest


from pyorient.exceptions import *
from pyorient import OrientSocket
from pyorient import OrientRecord
from pyorient.messages.database import *
from pyorient.messages.commands import *
from pyorient.messages.cluster import *
from pyorient.messages.records import *
from pyorient.constants import *

os.environ['DEBUG'] = "1"
os.environ['DEBUG_VERBOSE'] = "0"
if os.path.realpath( '../' ) not in sys.path:
    sys.path.insert( 0, os.path.realpath( '../' ) )

if os.path.realpath( '.' ) not in sys.path:
    sys.path.insert( 0, os.path.realpath( '.' ) )


class RawMessages_5_TestCase(unittest.TestCase):
    """ Command Test Case """

    def test_attach_class_hint(self):
        try:
            connection = OrientSocket( "localhost", 2424 )
            tx = TxCommitMessage(connection)
            tx.begin()
            tx.attach([1, 2, 3])
            assert False  # should not happens
        except AssertionError as e:
            assert 'A subclass of BaseMessage was expected' == str(e)
            assert True

    def test_private_prepare(self):
        try:
            connection = OrientSocket( "localhost", 2424 )
            DbOpenMessage( connection )\
                .prepare(
                    ("GratefulDeadConcerts", "admin", "admin", DB_TYPE_DOCUMENT, "")
                ).send().fetch_response()

            tx = TxCommitMessage(connection)
            tx.begin()
            tx.prepare()
            assert False
        except AttributeError as e:
            print(str(e))
            assert True


    def test_private_send(self):
        try:
            connection = OrientSocket( "localhost", 2424 )
            DbOpenMessage( connection )\
                .prepare(
                    ("GratefulDeadConcerts", "admin", "admin", DB_TYPE_DOCUMENT, "")
                ).send().fetch_response()
            tx = TxCommitMessage(connection)
            tx.begin()
            tx.send()
            assert False
        except AttributeError as e:
            print(str(e))
            assert True

    def test_private_fetch(self):
        try:
            connection = OrientSocket( "localhost", 2424 )
            DbOpenMessage( connection )\
                .prepare(
                    ("GratefulDeadConcerts", "admin", "admin", DB_TYPE_DOCUMENT, "")
                ).send().fetch_response()
            tx = TxCommitMessage(connection)
            tx.begin()
            tx.fetch_response()
            assert False
        except AttributeError as e:
            print( str(e))
            assert True

    def test_transaction(self):
        connection = OrientSocket( "localhost", 2424 )
        session_id = ( ConnectMessage( connection ) ).prepare( ("root", "root") )\
            .send().fetch_response()

        db_name = "my_little_test"

        msg = DbExistsMessage( connection )
        exists = msg.prepare( [db_name] ).send().fetch_response()

        print("Before %r" % exists)
        try:
            ( DbDropMessage( connection ) ).prepare([db_name]) \
                .send().fetch_response()
            assert True
        except PyOrientCommandException as e:
            print(str(e))
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
        except PyOrientCommandException as e:
            # class my_class already exists
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


        tx = TxCommitMessage(connection)
        tx.begin()
        tx.attach( rec_position1 )
        tx.attach( rec_position1 )
        tx.attach( rec_position2 )
        tx.attach( update_success )
        tx.attach( delete_msg )
        res = tx.commit()

        for k, v in res.items():
            print(k + " -> " + v.vacanza)

        assert len(res) == 4
        assert res["#3:0"].vacanza == 'montagna'
        assert res["#3:2"].vacanza == 'mare'
        assert res["#3:3"].vacanza == 'mare'
        assert res["#3:4"].vacanza == 'lago'

        sid = ( ConnectMessage( connection ) ).prepare( ("root", "root") )\
            .send().fetch_response()

        # # at the end drop the test database
        ( DbDropMessage( connection ) ).prepare([db_name, STORAGE_TYPE_MEMORY]) \
            .send().fetch_response()
