# -*- coding: utf-8 -*-
__author__ = 'Ostico <ostico@gmail.com>'
import unittest
import os

os.environ['DEBUG'] = "0"
os.environ['DEBUG_VERBOSE'] = "0"

import pyorient


class CommandTestCase( unittest.TestCase ):
    def __init__( self, *args, **kwargs ):
        super( CommandTestCase, self ).__init__( *args, **kwargs )
        self.client = None
        self.cluster_info = None
        self.class_id1 = None

    def setUp( self ):

        self.client = pyorient.OrientDB( "localhost", 2424 )
        self.client.connect( "admin", "admin" )

        db_name = "test_tr"
        try:
            self.client.db_drop( db_name )
        except pyorient.PyOrientCommandException as e:
            print( e )
        finally:
            db = self.client.db_create( db_name, pyorient.DB_TYPE_GRAPH,
                                        pyorient.STORAGE_TYPE_MEMORY )
            pass

        self.cluster_info = self.client.db_open(
            db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
        )

        self.class_id1 = \
            self.client.command( "create class my_v_class extends V" )[0]

    def test_boolean( self ):
        rec = self.client.command( 'create vertex v content {"abcdef":false,'
                                   '"qwerty":TRUE}' )
        assert rec[0].abcdef is not True, "abcdef expected False: '%s'" % rec[
            0].abcdef
        assert rec[0].qwerty is True, "qwerty expected True: '%s'" % rec[
            0].qwerty

        rec_value = self.client.query( 'select from v' )
        assert rec_value[0].abcdef is not True, "abcdef expected False: '%s'" % \
                                                rec_value[0].abcdef
        assert rec_value[0].qwerty is True, "qwerty expected True: '%s'" % \
                                            rec_value[0].qwerty

    def test_new_orient_dict( self ):
        import re

        rec = self.client.command( 'create vertex v content {"a":false,'
                                   '"q":TRUE}' )

        p = re.compile(
            "\{'@V':\{'[a|q]': (True|False), '[a|q]': (True|False)\}\}" )
        assert p.match( str( rec[0] ) ), \
            ( "Failed to assert that received " + str( rec[0] ) +
              " match to: {'@V':{'a': False, 'q': True}}" )

        rec = {'a': 1, 'b': 2, 'c': 3}
        rec_position = self.client.record_create( 3, rec )

        p = re.compile(
            "\{'[abc]': [123], '[abc]': [123], '[abc]': [123]\}" )
        assert p.match( str( rec_position ) ), \
            ("Failed to assert that received " + str(
                rec_position ) + " match to: "
                                 "{'a': 1, 'b': 2, 'c': 3}" )

        res = self.client.query( "select from " + rec_position.rid )
        assert p.match( str( res[0] ) ), \
            ("Failed to assert that received " + str(
                rec_position ) + " match to: "
                                 "{'a': 1, 'b': 2, 'c': 3}" )

        print( res[0].oRecordData['a'] )