__author__ = 'gremorian'

import unittest

import pyorient
import os
os.environ['DEBUG'] = "0"

old_token = ''


class TokenAuthTest(unittest.TestCase):
    """ Command Test Case """

    client = None

    def setUp(self):

        self.client = pyorient.OrientDB("localhost", 2424)
        client = pyorient.OrientDB("localhost", 2424)
        client.connect("root", "root")
        if client._connection.protocol < 26:
            self.skipTest("Token not supported in OrientDB < 2.0")

    def testPrepareConnection(self):
        global old_token

        self.client.set_session_token(True)
        self.client.db_open(
            "GratefulDeadConcerts", "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
        )
        record = self.client.query( 'select from V where @rid = #9:1' )
        assert isinstance( record[0], pyorient.types.OrientRecord )

        old_token = self.client.get_session_token()
        assert self.client.get_session_token() not in [
            None, '', b'', True, False
        ]

    def testReconnection(self):
        assert self.client.get_session_token() == b''
        global old_token
        self.client.set_session_token( old_token )
        record = self.client.query( 'select from V where @rid = #9:1' )
        assert isinstance( record[0], pyorient.types.OrientRecord )

    def testReconnectionFailRoot(self):
        assert self.client.get_session_token() == b''
        global old_token
        self.client.set_session_token( old_token )
        #
        # //this because the connection credentials
        # // are not correct for Orient root access
        self.assertRaises(
            pyorient.exceptions.PyOrientCommandException,
            self.client.db_exists,
            "GratefulDeadConcerts"
        )

    def testReconnectionRoot(self):
        assert self.client.get_session_token() == b''
        global old_token
        self.client.set_session_token( old_token )
        self.client.connect("root", "root")
        self.assertNotEquals( old_token, self.client.get_session_token() )
        res = self.client.db_exists( "GratefulDeadConcerts" )
        self.assertTrue( res )

    def testRenewAuthToken(self):
        assert self.client.get_session_token() == b''

        client = pyorient.OrientDB("localhost", 2424)
        client.set_session_token( True )
        client.db_open( "GratefulDeadConcerts", "admin", "admin" )
        res1 = client.record_load("#9:1")
        res2 = client.query( 'select from V where @rid = #9:1' )

        self.assertEqual(
            res1.oRecordData['name'],
            res2[0].oRecordData['name']
        )
        self.assertEqual(
            res1.oRecordData['out_sung_by'].get_hash(),
            res2[0].oRecordData['out_sung_by'].get_hash()
        )
        self.assertEqual(
            res1.oRecordData['out_written_by'].get_hash(),
            res2[0].oRecordData['out_written_by'].get_hash()
        )

        actual_token = client.get_session_token()
        del client

        #  create a new client
        client = pyorient.OrientDB("localhost", 2424)
        client.set_session_token( actual_token )
        res3 = client.query( 'select from V where @rid = #9:1' )

        self.assertEqual(
            res2[0].oRecordData['name'],
            res3[0].oRecordData['name']
        )
        self.assertEqual(
            res2[0].oRecordData['out_sung_by'].get_hash(),
            res3[0].oRecordData['out_sung_by'].get_hash()
        )
        self.assertEqual(
            res2[0].oRecordData['out_written_by'].get_hash(),
            res3[0].oRecordData['out_written_by'].get_hash()
        )

        # set the flag again to true if you want to renew the token
        # client = pyorient.OrientDB("localhost", 2424)
        client.set_session_token( True )
        client.db_open( "GratefulDeadConcerts", "admin", "admin" )

        global old_token
        self.assertNotEqual( old_token, actual_token )
        self.assertNotEqual( old_token, client.get_session_token() )
        self.assertNotEqual( actual_token, client.get_session_token() )
