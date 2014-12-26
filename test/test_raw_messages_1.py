import os
import sys
import unittest

from pyorient.exceptions import PyOrientCommandException, PyOrientConnectionException, PyOrientException
from pyorient import OrientSocket
from pyorient.messages.connection import ConnectMessage, ShutdownMessage
from pyorient.messages.database import DbExistsMessage, DbOpenMessage, DbCreateMessage,\
 DbDropMessage, DbReloadMessage, DbCloseMessage, DbSizeMessage, DbListMessage
from pyorient.messages.commands import CommandMessage
from pyorient.constants import DB_TYPE_DOCUMENT, QUERY_SYNC, \
    STORAGE_TYPE_PLOCAL


os.environ['DEBUG'] = "1"
if os.path.realpath( '../' ) not in sys.path:
    sys.path.insert( 0, os.path.realpath( '../' ) )

if os.path.realpath( '.' ) not in sys.path:
    sys.path.insert( 0, os.path.realpath( '.' ) )

# from pyorient.utils import *
# from pyorient.Messages.Constants.OrientPrimitives import *
# from OrientException import *



class RawMessages_1_TestCase(unittest.TestCase):
    """ Command Test Case """

    def test_not_singleton_socket(self):
        connection = OrientSocket( "localhost", 2424 )
        connection2 = OrientSocket( "localhost", 2424 )
        assert id(connection.get_connection()) != id(connection2.get_connection())

    def test_connection(self):
        connection = OrientSocket( "localhost", 2424 )
        msg = ConnectMessage( connection )
        print("%r" % msg.get_protocol())
        assert msg.get_protocol() != -1

        session_id = msg.prepare( ("root", "root") )\
            .send().fetch_response()
        """
        alternative use
            session_id = msg.set_user("admin").set_pass("admin").prepare()\
            .send().fetch_response()
        """

        assert session_id == connection.session_id
        assert session_id != -1

        msg.close()
        print("%r" % msg._header)
        print("%r" % session_id)

    def test_db_exists(self):

        connection = OrientSocket( "localhost", 2424 )
        msg = ConnectMessage( connection )
        print("%r" % msg.get_protocol())
        assert msg.get_protocol() != -1

        session_id = msg.prepare( ("root", "root") )\
            .send().fetch_response()

        print("Sid: %s" % session_id)
        assert session_id == connection.session_id
        assert session_id != -1

        db_name = "GratefulDeadConcerts"
        # params = ( db_name, STORAGE_TYPE_MEMORY )
        params = ( db_name, STORAGE_TYPE_PLOCAL )

        msg = DbExistsMessage( connection )

        exists = msg.prepare( params ).send().fetch_response()
        assert exists is True

        msg.close()
        print("%r" % exists)

    def test_db_open_connected(self):

        connection = OrientSocket( "localhost", 2424 )
        conn_msg = ConnectMessage( connection )

        print("%r" % conn_msg.get_protocol())
        assert conn_msg.get_protocol() != -1

        session_id = conn_msg.prepare( ("root", "root") )\
            .send().fetch_response()

        print("Sid: %s" % session_id)
        assert session_id == connection.session_id
        assert session_id != -1
        # ##################

        msg = DbOpenMessage( connection )

        db_name = "GratefulDeadConcerts"
        cluster_info = msg.prepare(
            (db_name, "admin", "admin", DB_TYPE_DOCUMENT, "")
        ).send().fetch_response()

        print("Cluster: %s" % cluster_info)
        assert len(cluster_info) != 0

        return connection, cluster_info

    def test_db_open_not_connected(self):

        connection = OrientSocket( "localhost", 2424 )

        print("Sid, should be -1 : %s" % connection.session_id)
        assert connection.session_id == -1

        # ##################

        msg = DbOpenMessage( connection )

        db_name = "GratefulDeadConcerts"
        cluster_info = msg.prepare(
            (db_name, "admin", "admin", DB_TYPE_DOCUMENT, "")
        ).send().fetch_response()

        print("Cluster: %s" % cluster_info)
        assert len(cluster_info) != 0
        return connection, cluster_info

    def test_db_create_without_connect(self):

        connection = OrientSocket( "localhost", 2424 )

        with self.assertRaises(PyOrientConnectionException):
            ( DbCreateMessage( connection ) ).prepare(
                ("db_test", DB_TYPE_DOCUMENT, STORAGE_TYPE_PLOCAL)
            ).send().fetch_response()

    def test_db_create_with_connect(self):

        connection = OrientSocket( "localhost", 2424 )
        conn_msg = ConnectMessage( connection )
        print("Protocol: %r" % conn_msg.get_protocol())

        session_id = conn_msg.prepare( ("root", "root") )\
            .send().fetch_response()

        print("Sid: %s" % session_id)
        assert session_id == connection.session_id
        assert session_id != -1

        # ##################

        db_name = "my_little_test"
        response = ''
        try:
            ( DbCreateMessage( connection ) ).prepare(
                (db_name, DB_TYPE_DOCUMENT, STORAGE_TYPE_PLOCAL)
            ).send().fetch_response()
        except PyOrientCommandException as e:
            assert True
            print(str(e))

        print("Creation: %r" % response)
        assert len(response) is 0

        msg = DbExistsMessage( connection )

        msg.prepare( (db_name, STORAGE_TYPE_PLOCAL) )
        # msg.prepare( [db_name] )
        exists = msg.send().fetch_response()
        assert exists is True

        msg.close()
        print("%r" % exists)

    def test_db_drop_without_connect(self):
        connection = OrientSocket( "localhost", 2424 )
        with self.assertRaises(PyOrientException):
            ( DbDropMessage( connection ) ).prepare(["test"]) \
                .send().fetch_response()

    def test_db_create_with_drop(self):

        connection = OrientSocket( "localhost", 2424 )
        conn_msg = ConnectMessage( connection )
        print("Protocol: %r" % conn_msg.get_protocol())
        assert connection.protocol != -1

        session_id = conn_msg.prepare( ("root", "root") ) \
            .send().fetch_response()

        print("Sid: %s" % session_id)
        assert session_id == connection.session_id
        assert session_id != -1

        # ##################

        db_name = "my_little_test"

        msg = DbExistsMessage( connection )
        exists = msg.prepare( [db_name] ).send().fetch_response()

        print("Before %r" % exists)

        assert exists is True  # should happen every time because of latest test
        if exists is True:
            ( DbDropMessage( connection ) ).prepare([db_name]) \
                .send().fetch_response()

        print("Creation again")
        try:
            ( DbCreateMessage( connection ) ).prepare(
                (db_name, DB_TYPE_DOCUMENT, STORAGE_TYPE_PLOCAL)
            ).send().fetch_response()
            assert True
        except PyOrientCommandException as e:
            print(str(e))
            assert False  # No expected Exception

        msg = DbExistsMessage( connection )
        exists = msg.prepare( [db_name] ).send().fetch_response()
        assert  exists is True

        # at the end drop the test database
        ( DbDropMessage( connection ) ).prepare([db_name]) \
            .send().fetch_response()

        msg.close()
        print("After %r" % exists)

    def test_db_close(self):
        connection = OrientSocket( "localhost", 2424 )
        conn_msg = ConnectMessage( connection )
        print("Protocol: %r" % conn_msg.get_protocol())
        assert connection.protocol != -1

        session_id = conn_msg.prepare( ("root", "root") ) \
            .send().fetch_response()

        print("Sid: %s" % session_id)
        assert session_id == connection.session_id
        assert session_id != -1

        c_msg = DbCloseMessage( connection )

        closing = c_msg.prepare(None)\
            .send().fetch_response()
        assert closing is 0

    def test_db_reload(self):

        connection, cluster_info = self.test_db_open_not_connected()

        reload_msg = DbReloadMessage( connection )
        cluster_reload = reload_msg.prepare().send().fetch_response()

        print("Cluster: %s" % cluster_info)
        assert cluster_info == cluster_reload

    def test_db_size(self):

        connection, cluster_info = self.test_db_open_not_connected()

        reload_msg = DbSizeMessage( connection )
        size = reload_msg.prepare().send().fetch_response()

        print("Size: %s" % size)
        assert size != 0

    def test_db_list(self):

        connection = OrientSocket( "localhost", 2424 )
        conn_msg = ConnectMessage( connection )

        print("%r" % conn_msg.get_protocol())
        assert conn_msg.get_protocol() != -1

        session_id = conn_msg.prepare( ("root", "root") )\
            .send().fetch_response()


        reload_msg = DbListMessage( connection )
        _list = reload_msg.prepare().send().fetch_response()

        print("Database List: %s" % _list.oRecordData['databases'] )
        assert len(_list.oRecordData['databases']) != 0

    def test_shutdown(self):

        import inspect
        print("# WARNING comment return below this line " \
              "to test this message. Line %u" % \
              inspect.currentframe().f_back.f_lineno)
        return

        connection = OrientSocket( "localhost", 2424 )
        msg = ConnectMessage( connection )
        print("%r" % msg.get_protocol())
        assert msg.get_protocol() != -1

        sid = msg.prepare( ("root", "root") )\
            .send().fetch_response()
        """
        alternative use
            session_id = msg.set_user("admin").set_pass("admin").prepare()\
            .send().fetch_response()
        """
        print("%r" % sid)
        assert sid != -1

        shut_msg = ShutdownMessage(connection)
        res = shut_msg.prepare(("root", "root")).\
            send().send().fetch_response()

        assert res[:] == []

    def test_command(self):
        connection = OrientSocket( "localhost", 2424 )

        print("Sid, should be -1 : %s" % connection.session_id)
        assert connection.session_id == -1

        # ##################

        msg = DbOpenMessage( connection )

        db_name = "GratefulDeadConcerts"
        cluster_info = msg.prepare(
            (db_name, "admin", "admin", DB_TYPE_DOCUMENT, "")
        ).send().fetch_response()
        assert len(cluster_info) != 0

        req_msg = CommandMessage( connection )

        res = req_msg.prepare( [ QUERY_SYNC, "select * from followed_by limit 1" ] ) \
            .send().fetch_response()

        print("%r" % res[0].rid)
        print("%r" % res[0].o_class)
        print("%r" % res[0].version)

