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
from pyorient.constants import DB_TYPE_DOCUMENT, QUERY_SYNC, \
    STORAGE_TYPE_PLOCAL, DB_TYPE_GRAPH, STORAGE_TYPE_MEMORY

os.environ['DEBUG'] = "0"
os.environ['DEBUG_VERBOSE'] = "0"
if os.path.realpath( '../' ) not in sys.path:
    sys.path.insert( 0, os.path.realpath( '../' ) )

if os.path.realpath( '.' ) not in sys.path:
    sys.path.insert( 0, os.path.realpath( '.' ) )




class RawMessages_2_TestCase(unittest.TestCase):
    """ Command Test Case """

    def test_record_object(self):
        x = OrientRecord()
        assert x.rid is None
        assert x.version is None
        assert x.o_class is None

    def test_record_load(self):
        connection = OrientSocket( "localhost", 2424 )

        assert connection.session_id == -1

        # ##################

        msg = DbOpenMessage( connection )

        db_name = "GratefulDeadConcerts"
        cluster_info = msg.prepare(
            (db_name, "admin", "admin", DB_TYPE_DOCUMENT, "")
        ).send().fetch_response()
        assert len(cluster_info) != 0

        def _test_callback(record):
            assert record is not []
            assert record.rid is not None  # assert no exception

        req_msg = RecordLoadMessage( connection )

        res = req_msg.prepare( [ "#11:0", "*:-1", _test_callback ] ) \
            .send().fetch_response()

        assert res.rid == "#11:0"
        assert res.o_class == 'followed_by'
        assert res._in != 0
        assert res._out != 0

    def test_record_count_with_no_opened_db(self):
        connection = OrientSocket( "localhost", 2424 )


        assert connection.session_id == -1

        # ##################
        conn_msg = ConnectMessage( connection )

        session_id = conn_msg.prepare( ("root", "root") )\
            .send().fetch_response()

        assert session_id == connection.session_id
        assert session_id != -1

        try:
            count_msg = DbCountRecordsMessage( connection )
            res = count_msg.prepare().send().fetch_response()
            assert False  # we expect an exception because we need a db opened
        except PyOrientDatabaseException:
            assert True


    def test_record_count(self):
        connection = OrientSocket( "localhost", 2424 )


        assert connection.session_id == -1

        # ##################

        msg = DbOpenMessage( connection )

        db_name = "GratefulDeadConcerts"
        cluster_info = msg.prepare(
            (db_name, "admin", "admin", DB_TYPE_DOCUMENT, "")
        ).send().fetch_response()
        assert len(cluster_info) != 0

        session_id = connection.session_id
        assert session_id != -1

        count_msg = DbCountRecordsMessage( connection )
        res = count_msg.prepare().send().fetch_response()

        assert res is not 0
        assert res > 0


    def test_record_create_update(self):

        connection = OrientSocket( "localhost", 2424 )
        conn_msg = ConnectMessage( connection )

        assert connection.protocol != -1

        session_id = conn_msg.prepare( ("root", "root") ) \
            .send().fetch_response()

        assert session_id == connection.session_id
        assert session_id != -1

        # ##################

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
                (db_name, DB_TYPE_GRAPH, STORAGE_TYPE_MEMORY)
            ).send().fetch_response()

        msg = DbOpenMessage( connection )
        cluster_info = msg.prepare(
            (db_name, "admin", "admin", DB_TYPE_GRAPH, "")
        ).send().fetch_response()
        assert len(cluster_info) != 0

        try:
            create_class = CommandMessage(connection)
            cluster = create_class.prepare((QUERY_CMD, "create class my_class "
                                                       "extends V"))\
                .send().fetch_response()[0]
        except PyOrientCommandException:
            # class my_class already exists
            pass

        # classes are not allowed in record create/update/load
        rec = { '@my_class': { 'alloggio': 'casa', 'lavoro': 'ufficio', 'vacanza': 'mare' } }
        rec_position = ( RecordCreateMessage(connection) )\
            .prepare( ( cluster, rec ) )\
            .send().fetch_response()

        print("New Rec Position: %s" % rec_position.rid)
        assert rec_position.rid is not None

        rec = { '@my_class': { 'alloggio': 'albergo', 'lavoro': 'ufficio', 'vacanza': 'montagna' } }
        update_success = ( RecordUpdateMessage(connection) )\
            .prepare( ( cluster, rec_position.rid, rec ) )\
            .send().fetch_response()

        assert update_success[0] != 0

        if connection.protocol <= 21:
            return unittest.skip("Protocol {!r} does not works well".format(
                connection.protocol ))  # skip test

        res = ( CommandMessage( connection ) )\
            .prepare( [ QUERY_SYNC, "select from " + rec_position.rid ] )\
            .send().fetch_response()

        # res = [ ( RecordLoadMessage(connection) ).prepare(
        #     [ rec_position.rid ]
        # ).send().fetch_response() ]

        print("%r" % res[0].rid)
        print("%r" % res[0].o_class)
        print("%r" % res[0].version)
        print("%r" % res[0].alloggio)
        print("%r" % res[0].lavoro)
        print("%r" % res[0].vacanza)

        assert res[0].rid == '#11:0'
        assert res[0].o_class == 'my_class'
        assert res[0].version >= 0
        assert res[0].alloggio == 'albergo'
        assert res[0].lavoro == 'ufficio'
        assert res[0].vacanza == 'montagna'

        sid = ( ConnectMessage( connection ) ).prepare( ("root", "root") ) \
            .send().fetch_response()

        # at the end drop the test database
        ( DbDropMessage( connection ) ).prepare([db_name]) \
            .send().fetch_response()


    def test_record_delete(self):

        connection = OrientSocket( "localhost", 2424 )

        conn_msg = ConnectMessage( connection )
        assert connection.protocol != -1

        session_id = conn_msg.prepare( ("root", "root") ) \
            .send().fetch_response()

        print("Sid: %s" % session_id)
        assert session_id == connection.session_id
        assert session_id != -1

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
                (db_name, DB_TYPE_DOCUMENT, STORAGE_TYPE_MEMORY)
            ).send().fetch_response()

        msg = DbOpenMessage( connection )
        cluster_info = msg.prepare(
            (db_name, "admin", "admin", DB_TYPE_DOCUMENT, "")
        ).send().fetch_response()

        assert len(cluster_info) != 0

        rec = { 'alloggio': 'casa', 'lavoro': 'ufficio', 'vacanza': 'mare' }
        rec_position = ( RecordCreateMessage(connection) )\
            .prepare( ( 1, rec ) )\
            .send().fetch_response()

        print("New Rec Position: %s" % rec_position.rid)
        assert rec_position.rid is not None

        ######################## Check Success
        res = ( CommandMessage( connection ) )\
            .prepare( [ QUERY_SYNC, "select from " + str(rec_position.rid) ] )\
            .send().fetch_response()

        assert res[0].rid == '#1:2'
        assert res[0].o_class is None
        assert res[0].version >= 0
        assert res[0].alloggio == 'casa'
        assert res[0].lavoro == 'ufficio'
        assert res[0].vacanza == 'mare'

        ######################## Delete Rid

        del_msg = (RecordDeleteMessage(connection))
        deletion = del_msg.prepare( ( 1, rec_position.rid ) )\
            .send().fetch_response()

        assert deletion is True

        # now try a failure in deletion for wrong rid
        del_msg = (RecordDeleteMessage(connection))
        deletion = del_msg.prepare( ( 1, 11111 ) )\
            .send().fetch_response()

        assert deletion is False

        sid = ( ConnectMessage( connection ) ).prepare( ("root", "root") ) \
            .send().fetch_response()

        # at the end drop the test database
        ( DbDropMessage( connection ) ).prepare([db_name]) \
            .send().fetch_response()


    def test_data_cluster_count(self):

        connection = OrientSocket( "localhost", 2424 )
        assert connection.session_id == -1

        # ##################

        msg = DbOpenMessage( connection )

        db_name = "GratefulDeadConcerts"
        cluster_info = msg.prepare(
            (db_name, "admin", "admin", DB_TYPE_DOCUMENT, "")
        ).send().fetch_response()

        print(cluster_info)
        assert len(cluster_info) != 0
        assert connection.session_id != -1

        count_msg = DataClusterCountMessage( connection )
        res1 = count_msg.set_count_tombstones(1)\
            .prepare( [ (0,1,2,3,4,5) ] ).send().fetch_response()

        assert res1 is not 0
        assert res1 > 0

        count_msg = DataClusterCountMessage( connection )
        res2 = count_msg.set_count_tombstones(1)\
            .prepare( [ (0,1,2,3,4,5), 1 ] ).send().fetch_response()


        assert res2 is not 0
        assert res2 > 0

        count_msg = DataClusterCountMessage( connection )
        res3 = count_msg.set_count_tombstones(1).set_cluster_ids( (0,1,2,3,4,5) )\
            .prepare().send().fetch_response()


        assert res3 is not 0
        assert res3 > 0

        assert res1 == res2
        assert res3 == res2
        assert res3 == res1

    def test_query_async(self):
        connection = OrientSocket( 'localhost', 2424 )
        open_msg = DbOpenMessage(connection)

        open_msg.set_db_name('GratefulDeadConcerts')\
            .set_user('admin').set_pass('admin').prepare()\
            .send().fetch_response()

        def _test_callback(record):
            assert record is not []
            assert record.rid is not None  # assert no exception

        try_select_async = CommandMessage(connection)

        try_select_async.set_command_type(QUERY_ASYNC)\
                        .set_query("select from followed_by")\
                        .set_limit(50)\
                        .set_fetch_plan("*:0")\
                        .set_callback( _test_callback )\
                        .prepare()\


        response = try_select_async.send().fetch_response()

        assert response is None

    def test_wrong_data_range(self):
        connection = OrientSocket( 'localhost', 2424 )

        db_name = "GratefulDeadConcerts"

        db = DbOpenMessage(connection)
        cluster_info = db.prepare(
            (db_name, "admin", "admin", DB_TYPE_DOCUMENT, "")
        ).send().fetch_response()

        datarange = DataClusterDataRangeMessage(connection)
        try:
            value = datarange.prepare(32767).send().fetch_response()
        except PyOrientCommandException as e:
            print(repr(str(e)))
            assert "java.lang.ArrayIndexOutOfBoundsException" in str(e)

    def test_data_range(self):
        connection = OrientSocket( 'localhost', 2424 )

        db_name = "GratefulDeadConcerts"

        db = DbOpenMessage(connection)
        cluster_info = db.prepare(
            (db_name, "admin", "admin", DB_TYPE_DOCUMENT, "")
        ).send().fetch_response()

        cluster_info.sort(key=lambda cluster: cluster['id'])

        for cluster in cluster_info:
            # os.environ['DEBUG'] = '0'  # silence debug
            datarange = DataClusterDataRangeMessage(connection)
            value = datarange.prepare(cluster['id']).send().fetch_response()
            print("Cluster Name: %s, ID: %u: %s " \
                  % ( cluster['name'], cluster['id'], value ))
            assert value is not []
            assert value is not None


# x = RawMessages_2_TestCase('test_wrong_data_range').run()
