__author__ = 'Ostico <ostico@gmail.com>'
import unittest
import os

os.environ['DEBUG'] = "0"
os.environ['DEBUG_VERBOSE'] = "0"
# if os.path.realpath('../') not in sys.path:
#     sys.path.insert(0, os.path.realpath('../'))
#
# if os.path.realpath('.') not in sys.path:
#     sys.path.insert(0, os.path.realpath('.'))

import pyorient
from pyorient import OrientRecord, OrientRecordLink

class CommandTestCase(unittest.TestCase):
    """ Command Test Case """

    def test_new_client_interface(self):

            client = pyorient.OrientDB("localhost", 2424)
            session_id = client.connect( "root", "root" )

            db_name = "GratefulDeadConcerts"

            cluster_info = client.db_open( db_name, "admin", "admin" )
            assert cluster_info != []

            result = client.query("select from followed_by", 10, '*:0')
            assert True
            assert result != []

            assert isinstance( result[0], OrientRecord )
            assert len(result) == 10
            assert isinstance(result[0]._in, OrientRecordLink )
            assert isinstance(result[0]._out, OrientRecordLink )
            assert result[0].weight == 1

            def _callback(item):
                self.assertTrue(item)
                assert isinstance( item, OrientRecord )

            result = client.query_async("select from followed_by",
                                        10, '*:0', _callback )
            assert True
            assert result is None

            res = client.record_load( "#11:0", "*:-1", _callback )
            assert res._rid == "#11:0"
            assert res._class == 'followed_by'
            assert isinstance(res._in, OrientRecordLink)
            assert isinstance(res._out, OrientRecordLink)

            session_id = client.connect( "root", "root" )

            # TEST COMMANDS
            db_name = 'test_commands'
            exists = client.db_exists( db_name, pyorient.STORAGE_TYPE_MEMORY )

            print("Before %r" % exists)
            try:
                client.db_drop(db_name)
                assert True
            except pyorient.PyOrientCommandException as e:
                print(str(e))
            finally:
                client.db_create( db_name, pyorient.DB_TYPE_GRAPH,
                                  pyorient.STORAGE_TYPE_MEMORY )

            cluster_info = client.db_open(
                db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
            )

            cluster_id = client.command( "create class my_class extends V" )[0]
            assert cluster_id != 0

            rec = { '@my_class': { 'alloggio': 'casa', 'lavoro': 'ufficio', 'vacanza': 'mare' } }
            rec_position = client.record_create( cluster_id, rec )

            print("New Rec Position: %s" % rec_position._rid)
            assert rec_position._rid is not None
            assert rec_position._rid != 0
            assert rec_position._rid != -1

            res = client.record_load( rec_position._rid, "*:0" )
            assert res._rid == rec_position._rid
            # assert res._class == 'my_class'
            assert res.alloggio == 'casa'
            assert res.lavoro == 'ufficio'
            assert res.vacanza == 'mare'

            deletion = client.record_delete( cluster_id, rec_position._rid )
            assert deletion is True

            result = client.query("select from my_class", 10, '*:0')
            assert True
            assert result == []

    def test_cluster_add_drop_recount(self):

        client = pyorient.OrientDB("localhost", 2424)  # TEST COMMANDS
        client.connect( "root", "root" )

        db_name = 'test_commands'
        exists = client.db_exists(db_name, pyorient.STORAGE_TYPE_MEMORY)

        print("Before %r" % exists)
        try:
            client.db_drop(db_name)
            assert True
        except pyorient.PyOrientCommandException as e:
            print(str(e))
        finally:
            client.db_create(db_name, pyorient.DB_TYPE_GRAPH,
                             pyorient.STORAGE_TYPE_MEMORY)

        cluster_info = client.db_open(
            db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
        )

        # CLUSTERS
        new_cluster_id = client.data_cluster_add(
            'my_cluster_1234567', pyorient.CLUSTER_TYPE_PHYSICAL
        )
        assert new_cluster_id > 0

        new_cluster_list = client.db_reload()
        new_cluster_list.sort(key=lambda cluster: cluster.id)

        _list = []
        for cluster in new_cluster_list:
            print("Cluster Name: %s, ID: %u " % (cluster.name, cluster.id))
            value = client.data_cluster_data_range(cluster.id)
            print("Value: %s " % value)
            _list.append(cluster.id)
            assert value is not []
            assert value is not None

        # check for new cluster in database
        try:
            _list.index(new_cluster_id)
            print("New cluster %r found in reload." % new_cluster_id)
            assert True
        except ValueError:
            assert False

        # delete the new cluster TODO: broken test
        print("Drop Cluster ID: %r" % new_cluster_id)
        drop_cluster = client.data_cluster_drop(new_cluster_id)
        assert drop_cluster is True



    def test_transaction_new_iface(self):

        client = pyorient.OrientDB('localhost', 2424)

        client.connect( "root", "root" )

        db_name = 'test_transactions'

        exists = client.db_exists( db_name, pyorient.STORAGE_TYPE_MEMORY )

        print("Before %r" % exists)
        try:
            client.db_drop(db_name)
            assert True
        except pyorient.PyOrientCommandException as e:
            print(str(e))
        finally:
            client.db_create( db_name, pyorient.DB_TYPE_GRAPH,
                              pyorient.STORAGE_TYPE_MEMORY )

        cluster_info = client.db_open(
            db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
        )

        #######################################

        # execute real create
        rec = { 'alloggio': 'baita', 'lavoro': 'no', 'vacanza': 'lago' }
        rec_position = client.record_create( 3, rec )

        #  START TRANSACTION
        print("debug breakpoint line")
        tx = client.tx_commit()
        tx.begin()

        # prepare for an update
        rec3 = { 'alloggio': 'albergo', 'lavoro': 'ufficio', 'vacanza': 'montagna' }
        tx_update1 = client.record_update( 3, rec_position._rid, rec3,
                                  rec_position._version )

        # prepare transaction
        rec1 = { 'alloggio': 'casa', 'lavoro': 'ufficio', 'vacanza': 'mare' }
        tx_create_1 = client.record_create( -1, rec1 )

        rec2 = { 'alloggio': 'baita', 'lavoro': 'no', 'vacanza': 'lago' }
        tx_create_2 = client.record_create( -1, rec2 )

        tx_delete_1 = client.record_delete( 3, rec_position._rid )

        tx.attach( tx_create_1 )
        tx.attach( tx_create_1 )
        tx.attach( tx_create_2 )
        tx.attach( tx_update1 )
        tx.attach( tx_delete_1 )
        res = tx.commit()

        for k, v in res.items():
            print(k + " -> " + v.vacanza)

        # in OrientDB version 2.2.9 transactions are executed in reverse order ( list pop )
        # in previous versions, instead, transaction are executed in crescent order ( list shift )
        assert len(res) == 3
        if client.version.major >= 2 and client.version.minor >= 2 and client.version.build < 9:
            assert res["#3:1"].vacanza == 'mare'
            assert res["#3:2"].vacanza == 'mare'
            assert res["#3:3"].vacanza == 'lago'
        else:
            assert res["#3:1"].vacanza == 'lago'
            assert res["#3:2"].vacanza == 'mare'
            assert res["#3:3"].vacanza == 'mare'

        client.connect( "root", "root" )
        client.db_drop( db_name, pyorient.STORAGE_TYPE_MEMORY )

    def test_reserved_words_and_batch_scripts(self):

        client = pyorient.OrientDB("localhost", 2424)
        client.connect("root", "root")

        if client._connection.protocol <= 21:
            return unittest.skip("Protocol {!r} does not works well".format(
                client._connection.protocol ))  # skip test

        db_name = "test_tr"
        try:
            client.db_drop(db_name)
        except pyorient.PyOrientCommandException as e:
            print(e)
        finally:
            db = client.db_create( db_name, pyorient.DB_TYPE_GRAPH,
                                   pyorient.STORAGE_TYPE_MEMORY )

        cluster_info = client.db_open(
            db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
        )

        class_id1 = client.command( "create class my_v_class extends V" )[0]
        class_id2 = client.command( "create class my_e_class extends E" )[0]
        rec1 = { '@my_v_class': { 'accommodation': 'house', 'work': 'office', 'holiday': 'sea' } }
        rec2 = { '@my_v_class': { 'accommodation': 'house', 'work2': 'office', 'holiday': 'sea3' } }
        rec_position1 = client.record_create(class_id1, rec1)
        rec_position2 = client.record_create(class_id1, rec2)
        sql_edge = "create edge from " + rec_position1._rid + " to " + rec_position2._rid
        res = client.command( sql_edge )

    def test_use_of_dir(self):
        client = pyorient.OrientDB("localhost", 2424)
        client.connect("root", "root")
        dir(client)

    def test_alter_statement(self):
        client = pyorient.OrientDB("localhost", 2424)
        client.connect("root", "root")

        db_name = "test_1234_db"
        try:
            client.db_drop(db_name)
        except pyorient.PyOrientCommandException as e:
            print(e)
        finally:
            db = client.db_create(db_name, pyorient.DB_TYPE_GRAPH,
                                  pyorient.STORAGE_TYPE_MEMORY)

        cluster_info = client.db_open(
            db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
        )

        client.command( "create class obj" )
        client.command( "create property obj._KEY string" )
        client.command( "alter property obj._KEY mandatory true" )
        with self.assertRaises( pyorient.PyOrientSQLParsingException ) as context:
            client.command( "create index KEY on obj _KEY unique" )

        self.assertTrue( 'Error '
                         in str( context.exception ) )
        self.assertTrue( 'parsing '
                         in str( context.exception ) )

        client.command( "create index KEY on obj ( _KEY ) unique" )
        assert True is True

    def test_limit(self):

        client = pyorient.OrientDB("localhost", 2424)
        client.connect("root", "root")

        db_name = "GratefulDeadConcerts"

        cluster_info = client.db_open(
            db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
        )

        assert len( client.query( "select from V Limit 1" ) ) == 1
        assert len( client.query( "select from V Limit 51" ) ) == 51
        assert len( client.query( "select from V lIMit 51" ) ) == 51
        assert len( client.query( "select from V LIMIT 51" ) ) == 51
        assert len( client.query( "select from V limit 51" ) ) == 51
        assert len( client.query( "select from V limit 1" ) ) == 1
        assert len( client.query( "select from V", 25 ) ) == 25
        assert len( client.query( "select from V limit 21", 10 ) ) == 21
        assert len( client.query( "select from V LIMIT 21", 10 ) ) == 21
        assert len( client.query( "select from V" ) ) == 20

# x = CommandTestCase('test_command').run()

# x = CommandTestCase('test_new_client_interface').run()
