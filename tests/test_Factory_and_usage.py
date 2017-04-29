__author__ = 'Ostico <ostico@gmail.com>'
import unittest
import os
import sys

os.environ['DEBUG'] = "1"
os.environ['DEBUG_VERBOSE'] = "0"
# if os.path.realpath('../') not in sys.path:
#     sys.path.insert(0, os.path.realpath('../'))
#
# if os.path.realpath('.') not in sys.path:
#     sys.path.insert(0, os.path.realpath('.'))

import pyorient

class CommandTestCase(unittest.TestCase):
    """ Command Test Case """

    def test_hi_level_interface(self):

        factory = pyorient.OrientDB('localhost', 2424)

        factory.get_message( pyorient.CONNECT ).prepare( ("root", "root") )\
            .send().fetch_response()

        db_name = 'demo_db'

        exists = factory.get_message( pyorient.DB_EXIST )\
            .prepare( [db_name, pyorient.STORAGE_TYPE_MEMORY] )\
            .send().fetch_response()

        print("Before %r" % exists)
        try:
            ( factory.get_message( pyorient.DB_DROP ) ).prepare([db_name]) \
                .send().fetch_response()
            assert True
        except pyorient.PyOrientCommandException as e:
            print(str(e))
        finally:
            ( factory.get_message( pyorient.DB_CREATE ) ).prepare(
                (db_name, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY )
            ).send().fetch_response()

        msg = factory.get_message( pyorient.DB_OPEN )
        clusters = msg.prepare(
            (db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, "")
        ).send().fetch_response()

        #######################################

        try:
            create_class = factory.get_message( pyorient.COMMAND )
            """:type create_class: pyorient.Messages.Database.CommandMessage"""
            create_class.prepare( ( pyorient.QUERY_CMD, "create class demo_class" ) )\
                .send().fetch_response()
            clusters = factory.get_message(pyorient.DB_RELOAD).prepare()\
                .send().fetch_response()
        except pyorient.PyOrientCommandException:
            pass

        from random import randint
        rec = { 'Nome': 'Dome', 'Cognome': 'Nico', 'test': str(randint(0, 999999999)) }

        insert_query = 'insert into demo_class ( {0} ) values( {1} )'.format(
            ",".join( rec.keys() ), "'" + "','".join( rec.values() ) + "'"
        )

        insert = factory.get_message( pyorient.COMMAND )\
            .prepare( ( pyorient.QUERY_CMD, insert_query ) )
        sql_insert_result = insert.send().fetch_response()

        cluster = 1
        for x in clusters:
            if x.name == 'demo_class':
                cluster = x.id
                break

        load = ( factory.get_message(pyorient.RECORD_CREATE) )\
            .prepare( [cluster, rec] )\
            .send().fetch_response()

        factory.get_message( pyorient.CONNECT ).prepare( ("root", "root") )\
            .send().fetch_response()

        drop_db_result = ( factory.get_message(pyorient.DB_DROP) )\
            .prepare(['demo_db', pyorient.STORAGE_TYPE_MEMORY])\
            .send().fetch_response()

        assert len( clusters ) != 0
        assert isinstance( sql_insert_result, list )
        assert len( sql_insert_result ) == 1
        assert isinstance( load, pyorient.OrientRecord )
        assert load._rid != -1
        assert isinstance( drop_db_result, list )
        assert len( drop_db_result ) == 0

    def test_hi_level_transaction(self):

        factory = pyorient.OrientDB('localhost', 2424)

        factory.get_message( pyorient.CONNECT ).prepare( ("root", "root") )\
            .send().fetch_response()

        db_name = 'test_transactions'

        exists = factory.get_message( pyorient.DB_EXIST )\
            .prepare( [db_name, pyorient.STORAGE_TYPE_MEMORY] )\
            .send().fetch_response()

        print("Before %r" % exists)
        try:
            ( factory.get_message( pyorient.DB_DROP ) ).prepare([db_name]) \
                .send().fetch_response()
            assert True
        except pyorient.PyOrientCommandException as e:
            print(str(e))
        finally:
            ( factory.get_message( pyorient.DB_CREATE ) ).prepare(
                (db_name, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY)
            ).send().fetch_response()

        msg = factory.get_message( pyorient.DB_OPEN )
        cluster_info = msg.prepare(
            (db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, "")
        ).send().fetch_response()

        #######################################

        # execute real create
        rec = { 'alloggio': 'baita', 'lavoro': 'no', 'vacanza': 'lago' }
        real_create_1 = ( factory.get_message(pyorient.RECORD_CREATE) )\
            .prepare( ( 3, rec ) )\
            .send().fetch_response()

        # create another real record
        rec = { 'alloggio': 'baita', 'lavoro': 'no', 'vacanza': 'deserto' }
        real_create_2 = ( factory.get_message(pyorient.RECORD_CREATE) )\
            .prepare( ( 3, rec ) )\
            .send().fetch_response()

        # prepare for an update
        rec3 = { 'alloggio': 'albergo', 'lavoro': 'ufficio', 'vacanza': 'montagna' }
        tx_update_1 = ( factory.get_message(pyorient.RECORD_UPDATE) )\
            .prepare( ( 3, real_create_1._rid, rec3, real_create_1._version ) )

        # prepare transaction
        rec1 = { 'alloggio': 'casa', 'lavoro': 'ufficio', 'vacanza': 'mare' }
        tx_create_1 = ( factory.get_message(pyorient.RECORD_CREATE) )\
            .prepare( ( -1, rec1 ) )

        rec2 = { 'alloggio': 'baita', 'lavoro': 'no', 'vacanza': 'lago' }
        tx_create_2 = ( factory.get_message(pyorient.RECORD_CREATE) )\
            .prepare( ( -1, rec2 ) )

        tx_delete_1_for_real_create_2 = ( factory.get_message(pyorient.RECORD_DELETE) )
        tx_delete_1_for_real_create_2.prepare( ( 3, real_create_2._rid ) )

        tx = ( factory.get_message(pyorient.TX_COMMIT) )
        tx.begin()
        tx.attach( tx_create_1 )
        tx.attach( tx_create_1 )
        tx.attach( tx_create_2 )
        tx.attach( tx_update_1 )
        tx.attach( tx_delete_1_for_real_create_2 )
        res = tx.commit()

        for k, v in res.items():
            print(k + " -> " + v.vacanza)

        # in OrientDB version 2.2.9 transactions are executed in reverse order ( list pop )
        # in previous versions, instead, transaction are executed in crescent order ( list shift )
        assert len(res) == 4
        if cluster_info[ 0 ].major >= 2 \
                and cluster_info[ 0 ].minor >= 2 \
                and cluster_info[ 0 ].build < 9:
            assert res["#3:0"].vacanza == 'montagna'
            assert res["#3:2"].vacanza == 'mare'
            assert res["#3:3"].vacanza == 'mare'
            assert res["#3:4"].vacanza == 'lago'
        else:
            assert res["#3:0"].vacanza == 'montagna'
            assert res["#3:2"].vacanza == 'lago'
            assert res["#3:3"].vacanza == 'mare'
            assert res["#3:4"].vacanza == 'mare'

        sid = ( factory.get_message(pyorient.CONNECT) ).prepare( ("root", "root") )\
            .send().fetch_response()

        ( factory.get_message(pyorient.DB_DROP) ).prepare(
            [db_name, pyorient.STORAGE_TYPE_MEMORY ]) \
            .send().fetch_response()

    def test_command(self):

        connection = pyorient.OrientSocket( "localhost", 2424 )

        factory = pyorient.OrientDB(connection)

        session_id = ( factory.get_message(pyorient.CONNECT) ).prepare( ("root", "root") )\
            .send().fetch_response()

        db_name = "tmp_test1"

        try:
            # at the end drop the test database
            ( factory.get_message(pyorient.DB_DROP) ).prepare([db_name, pyorient.STORAGE_TYPE_MEMORY]) \
                .send().fetch_response()

        except pyorient.PyOrientCommandException as e:
            print(str(e))
        finally:
            ( factory.get_message(pyorient.DB_CREATE) ).prepare(
                (db_name, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY)
            ).send().fetch_response()

        # open as serialize2binary
        msg = factory.get_message(pyorient.DB_OPEN)
        cluster_info = msg.prepare(
            (db_name, "admin", "admin", pyorient.DB_TYPE_DOCUMENT, "", pyorient.OrientSerialization.CSV)
        ).send().fetch_response()

        # ##################

        create_class = factory.get_message(pyorient.COMMAND)
        ins_msg1 = factory.get_message(pyorient.COMMAND)
        ins_msg2 = factory.get_message(pyorient.COMMAND)
        ins_msg3 = factory.get_message(pyorient.COMMAND)
        ins_msg4 = factory.get_message(pyorient.COMMAND)
        upd_msg5 = factory.get_message(pyorient.RECORD_UPDATE)

        req_msg = factory.get_message(pyorient.COMMAND)

        create_class.prepare( ( pyorient.QUERY_CMD, "create class c_test extends V" ) )
        ins_msg1.prepare( ( pyorient.QUERY_CMD, "insert into c_test ( Band, Song ) values( 'AC/DC', 'Hells Bells' )") )
        ins_msg2.prepare( ( pyorient.QUERY_CMD, "insert into c_test ( Band, Song ) values( 'AC/DC', 'Who Made Who' )") )
        ins_msg3.prepare( ( pyorient.QUERY_CMD, "insert into c_test ( Band, Song ) values( 'AC/DC', 'T.N.T.' )") )
        ins_msg4.prepare( ( pyorient.QUERY_CMD, "insert into c_test ( Band, Song ) values( 'AC/DC', 'High Voltage' )") )


        cluster = create_class.send().fetch_response()
        rec1 = ins_msg1.send().fetch_response()
        rec2 = ins_msg2.send().fetch_response()
        rec3 = ins_msg3.send().fetch_response()
        rec4 = ins_msg4.send().fetch_response()

        rec1 = rec1[0]
        upd_res = upd_msg5.prepare( ( rec1._rid, rec1._rid, { 'Band': 'Metallica', 'Song': 'One' } ) )\
            .send().fetch_response()

        res = req_msg.prepare( [ pyorient.QUERY_SYNC, "select from c_test" ] ).send().fetch_response()

        assert isinstance(cluster, list)
        assert rec1._rid == res[0]._rid
        assert rec1._version != res[0]._version
        assert res[0]._version == upd_res[0]._version

        assert len(res) == 4
        # assert res[0]._rid == '#11:0'
        assert res[0].Band == 'Metallica'
        assert res[0].Song == 'One'

        assert res[3].Song == 'High Voltage'

        # for x in res:
        #     print("############"
        #     print("%r" % x._rid
        #     print("%r" % x._class
        #     print("%r" % x._version
        #     print("%r" % x.Band
        #     print("%r" % x.Song


        # classes are allowed in record create/update/load
        rec = { '@c_test': { 'alloggio': 'casa', 'lavoro': 'ufficio', 'vacanza': 'mare' } }
        rec_position = ( factory.get_message(pyorient.RECORD_CREATE) )\
            .prepare( ( cluster[0], rec ) )\
            .send().fetch_response()

        print("New Rec Position: %s" % rec_position._rid)
        assert rec_position._rid is not None

        rec = { '@c_test': { 'alloggio': 'albergo', 'lavoro': 'ufficio', 'vacanza': 'montagna' } }
        update_success = ( factory.get_message(pyorient.RECORD_UPDATE) )\
            .prepare( ( rec_position._rid, rec_position._rid, rec ) )\
            .send().fetch_response()


        req_msg = factory.get_message(pyorient.RECORD_LOAD)
        res = req_msg.prepare( [ rec_position._rid, "*:-1" ] ) \
            .send().fetch_response()

        # print(res)
        # print(res._rid)
        # print(res._class)
        # print(res._version)
        # print(res.alloggio)
        # print(res.lavoro)
        # print(res.vacanza)

        # assert res._rid == "#11:4"
        # assert res._class == "c_test"
        assert res.alloggio == 'albergo'
        assert not hasattr( res, 'Band')
        assert not hasattr( res, 'Song')

        # print(""
        # # at the end drop the test database
        # ( DbDropMessage( connection ) ).prepare([db_name, STORAGE_TYPE_MEMORY]) \
        #     .send().fetch_response()


# x = CommandTestCase('test_command').run()