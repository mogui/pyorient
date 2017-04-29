__author__ = 'Ostico <ostico@gmail.com>'

import sys
import os
import unittest


from pyorient.exceptions import *
from pyorient import OrientSocket
from pyorient.messages.connection import *
from pyorient.messages.database import *
from pyorient.messages.commands import *
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
        rec0 = { 'alloggio': 'baita', 'lavoro': 'no', 'vacanza': 'lago' }
        real_record1 = ( RecordCreateMessage(connection) )\
            .prepare( ( 3, rec0 ) )\
            .send().fetch_response()

        #######################
        # prepare for an update
        rec3 = { 'alloggio': 'ciao', 'lavoro': 'ciao2', 'vacanza': 'ciao3' }
        temp_update_real_rec = ( RecordUpdateMessage(connection) )\
            .prepare( ( 3, real_record1._rid, rec3, real_record1._version ) )

        # prepare transaction
        rec1 = { 'alloggio': 'casa', 'lavoro': 'ufficio', 'vacanza': 'mare' }
        temp_record1 = ( RecordCreateMessage(connection) )\
            .prepare( ( -1, rec1 ) )

        rec2 = { 'alloggio': 'baita', 'lavoro': 'no', 'vacanza': 'lago' }
        temp_record2 = ( RecordCreateMessage(connection) )\
            .prepare( ( -1, rec2 ) )

        delete_real_rec = RecordDeleteMessage(connection)
        delete_real_rec.prepare( ( 3, real_record1._rid ) )
        #######################

        # create another real record
        rec = { 'alloggio': 'bim', 'lavoro': 'bum', 'vacanza': 'bam' }
        real_record2 = ( RecordCreateMessage(connection) )\
            .prepare( ( 3, rec ) )\
            .send().fetch_response()

        tx = TxCommitMessage(connection)
        tx.begin()
        tx.attach( temp_record1 )
        tx.attach( temp_record2 )
        tx.attach( temp_update_real_rec )
        tx.attach( delete_real_rec )
        res = tx.commit()

        for k, v in res.items():
            print(k + " -> " + v.vacanza)

        # in OrientDB version 2.2.9 transactions are executed in reverse order ( list pop )
        # in previous versions, instead, transaction are executed in crescent order ( list shift )
        assert len(res) == 2
        if cluster_info[ 0 ].major >= 2 and cluster_info[ 0 ].minor >= 2 and cluster_info[ 0 ].build < 9:
            assert res["#3:2"].vacanza == 'mare'
            assert res["#3:3"].vacanza == 'lago'
        else:
            assert res["#3:2"].vacanza == 'lago'
            assert res["#3:3"].vacanza == 'mare'

        sid = ( ConnectMessage( connection ) ).prepare( ("root", "root") )\
            .send().fetch_response()

        # # at the end drop the test database
        ( DbDropMessage( connection ) ).prepare([db_name, STORAGE_TYPE_MEMORY]) \
            .send().fetch_response()
