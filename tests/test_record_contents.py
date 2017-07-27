# -*- coding: utf-8 -*-
__author__ = 'Ostico <ostico@gmail.com>'
import unittest
import os

os.environ['DEBUG'] = "1"
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
        self.client.connect( "root", "root" )

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
            db_name, "root", "root", pyorient.DB_TYPE_GRAPH, ""
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

    def test_record_create_nonstrings( self ):
        # this should succeed with no exception
        self.client.record_create (self.class_id1, {'@my_v_class': {'a': 1.5, 'b': 'foo'}} )

    def test_record_create_embedded_list( self ):
        # this should succeed with no exception
        self.client.record_create(self.class_id1, {'@my_v_class': {'a': ['bar', 'bar']}} )

    def test_record_create_embedded_dictionary( self ):
        # this should succeed with no exception
        self.client.record_create(self.class_id1, {'@my_v_class': {'a': [{'bar': 'bar'}]}} )

    def test_new_orient_dict( self ):
        import re

        rec = self.client.command( 'create vertex v content {"a":false,'
                                   '"q":TRUE}' )

        self.assertIn('a', rec[0])
        self.assertIn('q', rec[0])
        assert rec[0].a is False
        assert rec[0].q is True
        import re
        # this can differ from orientDB versions, so i use a regular expression
        assert re.match( '[0-1]', str( rec[0]._version ) )
        assert rec[0]._rid == '#9:0'

        rec = {'a': 1, 'b': 2, 'c': 3}
        rec_position = self.client.record_create( 3, rec )

        assert rec_position.a == 1
        assert rec_position.b == 2
        assert rec_position.c == 3
        # this can differ from orientDB versions, so i use a regular expression
        assert re.match( '[0-1]', str( rec_position._version ) )
        assert rec_position._rid == '#3:0'

        res = self.client.query( "select from " + rec_position._rid )
        assert res[0].a == 1
        assert res[0].b == 2
        assert res[0].c == 3
        # this can differ from orientDB versions, so i use a regular expression
        assert re.match( '[0-1]', str( res[0]._version ) )
        assert res[0]._rid == '#3:0'

        hashed_records = {rec_position: 1, res[0]: 2}
        self.assertEqual(len(hashed_records), 1)

        print( res[0].oRecordData['a'] )

    def test_embedded_map(self):

        res = self.client.command(
            'create vertex v content {"a":1,"b":{"d":"e"},"c":3}'
        )

        # print(res[0])

        res = self.client.command(
            'create vertex v content {"a":1,"b":{},"c":3}'
        )

        # print(res[0])
        # print(res[0].oRecordData['b'])
        assert res[0].oRecordData['b'] == {}, "Failed to asert that received " + \
                                         res[0].oRecordData['b'] + " equals '{}"

        res = self.client.command('create vertex v content {"a":1,"b":{}}')
        # print(res[0])
        assert res[0].oRecordData['b'] == {}, "Failed to asert that received " \
                                              "" + res[0].oRecordData['b'] + \
                                              " equals '{}"

        res = self.client.command(
            'create vertex v content {"b":{},"a":1,"d":{}}'
        )
        # print(res[0])

        assert res[0].oRecordData['b'] == {}, "Failed to asert that received " \
                                              "" + res[0].oRecordData['b'] + \
                                              " equals '{}"
        assert res[0].oRecordData['d'] == {}, "Failed to asert that received " \
                                              "" + res[0].oRecordData['d'] + \
                                              " equals '{}"

    def test_nested_objects_1(self):

        res = self.client.command(
            'create vertex v content {"b":[[1]],"a":{},"d":[12],"c":["x"]}'
        )
        print(res[0])

    def test_nested_objects_2(self):

        res = self.client.command(
            'create vertex v content {"b":[[1,"abc"]]}'
        )
        print(res[0])
        assert res[0].oRecordData['b'][0][0] == 1
        assert res[0].oRecordData['b'][0][1] == "abc"

    def test_nested_objects_3(self):

        res = self.client.command(
            'create vertex v content {"b":[[1,{"abc":2}]]}'
        )
        print(res[0])
        assert res[0].oRecordData['b'][0][0] == 1
        assert res[0].oRecordData['b'][0][1]['abc'] == 2

    def test_nested_objects_4(self):

        res = self.client.command(
            'create vertex v content {"b":[[1,{"abc":2}],[3,{"cde":4}]]}'
        )
        print(res[0])
        assert res[0].oRecordData['b'][0][0] == 1
        assert res[0].oRecordData['b'][0][1]['abc'] == 2
        assert res[0].oRecordData['b'][1][0] == 3
        assert res[0].oRecordData['b'][1][1]['cde'] == 4

    def test_nested_objects_5(self):
        res = self.client.command(
            'create vertex v content '
            '{"b":[[1,{"dx":[1,2]},"abc"]],"a":{},"d":[12],"c":["x"],"s":111}'
        )
        assert res[0].oRecordData['b'][0][0] == 1
        assert res[0].oRecordData['b'][0][1]['dx'][0] == 1
        assert res[0].oRecordData['b'][0][1]['dx'][1] == 2
        assert res[0].oRecordData['b'][0][2] == "abc"
        assert res[0].oRecordData['a'] == {}
        assert res[0].oRecordData['d'][0] == 12
        assert res[0].oRecordData['c'][0] == "x"
        assert res[0].oRecordData['s'] == 111

        print(res[0])

    def test_nested_objects_6(self):
        res = self.client.command(
            'create vertex v content '
            '{"b":[[1,2,"abc"]]}'
        )
        assert res[0].oRecordData['b'][0][0] == 1
        assert res[0].oRecordData['b'][0][1] == 2
        assert res[0].oRecordData['b'][0][2] == "abc"

        print(res[0])

    def test_nested_objects_7(self):
        res = self.client.command(
            'create vertex v content '
            '{"b":[{"xx":{"xxx":[1,2,"abc"]}}]}'
        )
        assert isinstance(res[0].oRecordData['b'], list)
        assert isinstance(res[0].oRecordData['b'][0], dict)
        assert isinstance(res[0].oRecordData['b'][0]['xx'], dict)
        assert isinstance(res[0].oRecordData['b'][0]['xx']['xxx'], list)

        assert res[0].oRecordData['b'][0]['xx']['xxx'][0] == 1
        assert res[0].oRecordData['b'][0]['xx']['xxx'][1] == 2
        assert res[0].oRecordData['b'][0]['xx']['xxx'][2] == "abc"

        print(res[0])

    def test_nested_objects_8(self):
        res = self.client.command(
            'create vertex v content '
            '{"b":[{"xx":{"xxx":[1,2,"abc"]}}],"c":[{"yy":{"yyy":[3,4,"cde"]}}]}'
        )
        assert isinstance(res[0].oRecordData['b'], list)
        assert isinstance(res[0].oRecordData['b'][0], dict)
        assert isinstance(res[0].oRecordData['b'][0]['xx'], dict)
        assert isinstance(res[0].oRecordData['b'][0]['xx']['xxx'], list)

        assert res[0].oRecordData['b'][0]['xx']['xxx'][0] == 1
        assert res[0].oRecordData['b'][0]['xx']['xxx'][1] == 2
        assert res[0].oRecordData['b'][0]['xx']['xxx'][2] == "abc"

        assert isinstance(res[0].oRecordData['c'], list)
        assert isinstance(res[0].oRecordData['c'][0], dict)
        assert isinstance(res[0].oRecordData['c'][0]['yy'], dict)
        assert isinstance(res[0].oRecordData['c'][0]['yy']['yyy'], list)

        assert res[0].oRecordData['c'][0]['yy']['yyy'][0] == 3
        assert res[0].oRecordData['c'][0]['yy']['yyy'][1] == 4
        assert res[0].oRecordData['c'][0]['yy']['yyy'][2] == "cde"

        print(res[0])

    def test_nested_objects_9(self):
        res = self.client.command(
            'create vertex v content '
            '{"a":[[1,2],[3,4],[5,6],null]}'
        )
        assert isinstance(res[0].oRecordData['a'], list)
        assert isinstance(res[0].oRecordData['a'][0], list)
        assert isinstance(res[0].oRecordData['a'][1], list)
        assert isinstance(res[0].oRecordData['a'][2], list)

        assert res[0].oRecordData['a'][0][0] == 1
        assert res[0].oRecordData['a'][0][1] == 2

        print(res[0])

    def test_nested_objects_10(self):
        res = self.client.command(
            'create vertex v content '
            '{"embedded_map":{"one":[1,2]}}'
        )

        assert isinstance(res[0].oRecordData['embedded_map'], dict)
        assert isinstance(res[0].oRecordData['embedded_map']['one'], list)

        assert res[0].oRecordData['embedded_map']['one'][0] == 1
        assert res[0].oRecordData['embedded_map']['one'][1] == 2

        print(res[0])

    def test_nested_objects_11(self):
        res = self.client.command(
            'create vertex v content '
            '{"embedded_map":{"one":{"three":4}}}'
        )

        assert isinstance(res[0].oRecordData['embedded_map'], dict)
        assert isinstance(res[0].oRecordData['embedded_map']['one'], dict)

        assert res[0].oRecordData['embedded_map']['one']["three"] == 4

        print(res[0])

    def test_nested_objects_12(self):
        res = self.client.command(
            'create vertex v content '
            '{"embedded_map":{"one":2}}'
        )

        assert isinstance(res[0].oRecordData['embedded_map'], dict)
        assert res[0].oRecordData['embedded_map']['one'] == 2

        print(res[0])

    def test_nested_objects_13(self):
        res = self.client.command(
            'create vertex v content '
            '{"a":1,"b":{},"c":3}'
        )

        assert res[0].oRecordData['a'] == 1
        assert isinstance(res[0].oRecordData['b'], dict)
        assert len(res[0].oRecordData['b']) == 0
        assert res[0].oRecordData['c'] == 3

        print(res[0])

    def test_quotes(self):
        import json
        test_data = {'scenario': 'a "quote" follows'}

        record = self.client.command("CREATE VERTEX V CONTENT " +
                                     json.dumps(test_data))[0]

        assert record._rid == '#9:0'
        assert record.oRecordData['scenario'] == 'a "quote" follows'

    def test_db_list(self):
        self.client.connect( "root", "root" )
        databases = self.client.db_list()
        assert databases.oRecordData[ 'databases' ][ 'GratefulDeadConcerts' ]

    def test_datetime(self):
        x = self.client.query(
            "SELECT DATE('2015-01-02 03:04:05')"
        )

        x = x[0].oRecordData

        import datetime
        assert 'DATE' in x
        assert isinstance( x['DATE'], datetime.datetime )
        assert str( x['DATE'] ) == '2015-01-02 03:04:05'

    def test_deserialize_numeric_types(self):

        lon1 = self.client.command(
            "CREATE VERTEX V CONTENT {'longitude': 1.1}")[0].longitude
        lon2 = self.client.command(
            "CREATE VERTEX V CONTENT {'longitude': -1.1}")[0].longitude
        lon3 = self.client.command(
            "CREATE VERTEX V CONTENT {'longNum': 5356336298435356336}"
        )[0].longNum
        lon4 = self.client.command(
            "CREATE VERTEX V CONTENT {'sciNum': 6.022E23}"
        )[0].sciNum
        lon5 = self.client.command(
            "CREATE VERTEX V CONTENT {'sciNum': 6.022E-23}"
        )[0].sciNum

        assert isinstance(lon1, float), \
            "type(lon1) is not equal to 'float': %r" % type(lon1)
        assert isinstance(lon2, float), \
            "type(lon2) is not equal to 'float': %r" % type(lon2)
        assert isinstance(lon4, float), \
            "type(lon4) is not equal to 'float': %r" % type(lon4)
        assert isinstance(lon5, float), \
            "type(lon5) is not equal to 'float': %r" % type(lon5)

        import sys
        if sys.version_info[0] < 3:
            assert isinstance(lon3, long), \
                "type(lon3) is not equal to 'long': %r" \
                % type(lon3)  # python 2.x long type
        else:
            assert isinstance(lon3, int), \
                "type(lon3) is not equal to 'int': %r" \
                % type(lon3)
