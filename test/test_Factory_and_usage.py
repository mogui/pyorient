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

        factory = pyorient.OrientDBFactory('localhost', 2424)

        factory.get_message( pyorient.CONNECT ).prepare( ("admin", "admin") )\
            .send().fetch_response()

        exists = factory.get_message( pyorient.DB_EXIST )\
            .prepare( ['demo_db', pyorient.STORAGE_TYPE_MEMORY] )\
            .send().fetch_response()

        if exists is True:
            open_msg = factory.get_message( pyorient.DB_OPEN )
            clusters = open_msg.prepare( ( 'demo_db', 'admin', 'admin' ) )\
                .send().fetch_response()
        else:
            create_msg = factory.get_message( pyorient.DB_CREATE )
            """:type create_msg: pyorient.Messages.Server.DbCreateMessage """
            clusters = create_msg.prepare(
                ( 'demo_db', pyorient.DB_TYPE_DOCUMENT, pyorient.STORAGE_TYPE_MEMORY )
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
            if x['name'] == 'demo_class':
                cluster = x['id']
                break

        load = ( factory.get_message(pyorient.RECORD_CREATE) )\
            .prepare( [cluster, rec] )\
            .send().fetch_response()


        drop_db_result = ( factory.get_message(pyorient.DB_DROP) )\
            .prepare(['demo_db', pyorient.STORAGE_TYPE_MEMORY])\
            .send().fetch_response()

        # print clusters
        # print sql_insert_result
        # print load.rid
        # print drop_db_result

        assert isinstance( clusters, list )
        assert len( clusters ) != 0
        assert isinstance( sql_insert_result, list )
        assert len( sql_insert_result ) == 0
        assert isinstance( load, pyorient.OrientRecord )
        assert load.rid != -1
        assert isinstance( drop_db_result, list )
        assert len( drop_db_result ) == 0

    def test_hi_level_transaction(self):

        factory = pyorient.OrientDBFactory('localhost', 2424)

        factory.get_message( pyorient.CONNECT ).prepare( ("admin", "admin") )\
            .send().fetch_response()

        exists = factory.get_message( pyorient.DB_EXIST )\
            .prepare( ['test_tx', pyorient.STORAGE_TYPE_MEMORY] )\
            .send().fetch_response()

        if exists is True:
            open_msg = factory.get_message( pyorient.DB_OPEN )
            clusters = open_msg.prepare( ( 'demo_db', 'admin', 'admin' ) )\
                .send().fetch_response()
        else:
            create_msg = factory.get_message( pyorient.DB_CREATE )
            """:type create_msg: pyorient.Messages.Server.DbCreateMessage """
            clusters = create_msg.prepare(
                ( 'demo_db', pyorient.DB_TYPE_DOCUMENT, pyorient.STORAGE_TYPE_MEMORY )
            ).send().fetch_response()

            #######################################

        # execute real create
        rec = { 'alloggio': 'baita', 'lavoro': 'no', 'vacanza': 'lago' }
        rec_position = ( factory.get_message(pyorient.RECORD_CREATE) )\
            .prepare( ( 3, rec ) )\
            .send().fetch_response()

        # prepare for an update
        rec3 = { 'alloggio': 'albergo', 'lavoro': 'ufficio', 'vacanza': 'montagna' }
        update_success = ( factory.get_message(pyorient.RECORD_UPDATE) )\
            .prepare( ( 3, rec_position.rid, rec3, rec_position.version ) )

        # prepare transaction
        rec1 = { 'alloggio': 'casa', 'lavoro': 'ufficio', 'vacanza': 'mare' }
        rec_position1 = ( factory.get_message(pyorient.RECORD_CREATE) )\
            .prepare( ( -1, rec1 ) )

        rec2 = { 'alloggio': 'baita', 'lavoro': 'no', 'vacanza': 'lago' }
        rec_position2 = ( factory.get_message(pyorient.RECORD_CREATE) )\
            .prepare( ( -1, rec2 ) )


        # create another real record
        rec = { 'alloggio': 'baita', 'lavoro': 'no', 'vacanza': 'lago' }
        rec_position = ( factory.get_message(pyorient.RECORD_CREATE) )\
            .prepare( ( 3, rec ) )\
            .send().fetch_response()

        delete_msg = ( factory.get_message(pyorient.RECORD_DELETE) )
        delete_msg.prepare( ( 3, rec_position.rid ) )


        tx = ( factory.get_message(pyorient.TX_COMMIT) )
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

        ( factory.get_message(pyorient.DB_DROP) ).prepare(
            ['demo_db', pyorient.STORAGE_TYPE_MEMORY ]) \
            .send().fetch_response()
