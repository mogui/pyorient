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
from pyorient.Messages.Database.TXCommitMessage import TXCommitMessage
from pyorient.Commons.OrientTypes import *


class CommandTestCase(unittest.TestCase):
    """ Command Test Case """

    def test_transaction(self):
        connection = OrientSocket( "localhost", int( 2424 ) )
        session_id = ( ConnectMessage( connection ) ).prepare( ("admin", "admin") )\
            .send().fetch_response()

        db_name = "my_little_test"

        msg = DbExistsMessage( connection )
        exists = msg.prepare( [db_name] ).send().fetch_response()

        print "Before %r" % exists
        if exists is False:
            print "Creation"
            try:
                ( DbCreateMessage( connection ) ).prepare(
                    (db_name, DB_TYPE_DOCUMENT, STORAGE_TYPE_PLOCAL)
                ).send().fetch_response()
                assert True
            except PyOrientCommandException, e:
                print e.message
                assert False  # No expected Exception
        else:
            msg = DbOpenMessage( connection )
            cluster_info = msg.prepare(
                (db_name, "admin", "admin", DB_TYPE_DOCUMENT, "")
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



        # prepare transaction
        rec1 = { 'alloggio': 'casa', 'lavoro': 'ufficio', 'vacanza': 'mare' }
        rec_position1 = ( RecordCreateMessage(connection) )\
            .prepare( ( -1, rec1 ) )

        # update old record
        rec2 = { 'alloggio': 'albergo', 'lavoro': 'ufficio', 'vacanza': 'montagna' }
        update_success = ( RecordUpdateMessage(connection) )\
            .prepare( ( 3, rec_position[0].rid, rec2, rec_position[0].version ) )

        delete_msg = RecordDeleteMessage(connection)
        r = delete_msg.prepare(( 3, rec_position[0].rid ))

        tx = TXCommitMessage(connection)
        tx.begin()
        tx.append( rec_position1 )
        tx.append( rec_position1 )
        tx.append( update_success )
        tx.append( delete_msg )
        res = tx.commit()

        assert res == { 'changes': [],
                        'created': [{'client_c_id': -1,
                                     'client_c_pos': -3,
                                     'created_c_id': 3,
                                     'created_c_pos': 2},
                                    {'client_c_id': -1,
                                     'client_c_pos': -2,
                                     'created_c_id': 3,
                                     'created_c_pos': 1}],
                        'updated': [{'new_version': 1, 'updated_c_id': 3,
                                     'updated_c_pos': 2},
                                    {'new_version': 1, 'updated_c_id': 3,
                                     'updated_c_pos': 1}]}

        PrettyPrinter(indent=2).pprint( res )

        # print ""
        # # at the end drop the test database
        ( DbDropMessage( connection ) ).prepare([db_name, STORAGE_TYPE_PLOCAL]) \
            .send().fetch_response()



# test_transaction()