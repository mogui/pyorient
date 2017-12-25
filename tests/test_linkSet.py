__author__ = 'gremorian'

import unittest

import pyorient


class LinkSetTestCase(unittest.TestCase):
    """ Command Test Case """

    def setUp(self):

        self.client = pyorient.OrientDB("localhost", 2424)
        self.client.connect("root", "root")

        db_name = "test_set"
        try:
            self.client.db_drop(db_name)
        except pyorient.PyOrientCommandException as e:
            print(e)
        finally:
            db = self.client.db_create(db_name, pyorient.DB_TYPE_GRAPH,
                                       pyorient.STORAGE_TYPE_MEMORY)
            pass

        self.client.db_open(
            db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
        )

        self._links_cluster_id = self.client.command("create class links "
                                                     "extends V")[0]
        self._sites_cluster_id = self.client.command("create class sites "
                                                     "extends V")[0]

        self.client.command(
            "create vertex sites set name = 'linkedin', id = 1 ")
        self.client.command("create vertex sites set name = 'google', id = 2 ")
        self.client.command("create vertex sites set name = 'github', id = 3 ")

        self.client.command("create vertex links set name = 'link1', "
                            "value = "
                            "'https://github.com/mogui/pyorient/issues', "
                            "id = 1, siteId = 3")
        self.client.command("create vertex links set name = 'link2', "
                            "value = "
                            "'https://github.com/mogui/pyorient/pulls', "
                            "id = 2, siteId = 3")
        self.client.command("create vertex links set name = 'link3', "
                            "value = "
                            "'https://github.com/mogui/pyorient/pulse', "
                            "id = 3, siteId = 3")
        self.client.command("create vertex links set name = 'link4', "
                            "value = "
                            "'https://github.com/mogui/pyorient/graphs', "
                            "id = 4, siteId = 3")

        self.client.command(
            "CREATE LINK link TYPE LINKSET FROM links.siteId TO "
            "sites.id INVERSE")

    def test_read_LinkSet(self):
        res = self.client.query("SELECT FROM sites where id = 3")
        self.assertIn('link', res[0])
        assert len(res[0].oRecordData['link']) == 4
        for link in res[0].oRecordData['link']:
            # can no more cheeck on the fixed clustedID, so the "only thing"(?) i can assert is not empty rid
            # because i think this should not test the database but the driver
            import re
            assert re.match('#[-]*[0-9]+:[0-9]+', link.get_hash() ), (
                "Failed to assert that "
                "'#[-]*[0-9]+:[0-9]+' matches received "
                "value: '%s'" % link.get_hash()
            )

    def test_oUser(self):
        res = self.client.query("select from oUser")
        assert len(res) == 3
        for user in res:
            assert user.oRecordData['roles'][0].clusterID == '4'

    def testEmbed(self):

        self._links_cluster_id = self.client.command("create class test_embed "
                                                     "extends V")[0]

        self.client.command("create vertex test_embed "
                            "set embMap = "
                            "{'en': 'english','it':'italian', 'ru': 'russian'}")

        x = self.client.query("SELECT embMap.keys() FROM test_embed")[0].oRecordData

        assert 'embMap' in x
        assert 'it' in x['embMap']
        assert 'en' in x['embMap']
        assert 'ru' in x['embMap']

        x = self.client.query(
            "SELECT embMap.keys().asString() FROM ( select from test_embed limit 1 )"
        )[0].oRecordData

        assert 'embMap' in x
        assert 'it' in x['embMap'] and 'ru' in x['embMap'] and 'en' in x['embMap']

    def testEmbedNum(self):

        self._links_cluster_id = self.client.command("create class test_embed "
                                                     "extends V")[0]

        self.client.command("create vertex test_embed "
                            "set embMap = "
                            "{'en': 1,'it':2, 'ru':3}")

        x = self.client.query("SELECT embMap.keys() FROM test_embed")[0].oRecordData

        assert 'embMap' in x
        assert 'en' in x['embMap']
        assert 'it' in x['embMap']
        assert 'ru' in x['embMap']

        x = self.client.query(
            "SELECT embMap.keys().asString() FROM ( select from test_embed limit 1 )"
        )[0].oRecordData

        assert 'embMap' in x
        assert ('en' in x['embMap'] and 'it' in x['embMap'] and 'ru' in x['embMap'])

    def testEmbeddedMapsInList(self):
        class_id1 = self.client.command("CREATE VERTEX V SET mapInList = "
                                        "[ {'one': 2, 'three': 4 } ]"
                                        )[0].oRecordData

        assert 'mapInList' in class_id1
        assert len(class_id1[ 'mapInList' ]) == 1
        assert class_id1[ 'mapInList' ][0]['one'] == 2
        assert class_id1[ 'mapInList' ][0]['three'] == 4

    def testEmptyEmbeddedMapsInList(self):
        # self.skipTest('Bug')
        print(1)
        class_id1 = self.client.command("CREATE VERTEX V CONTENT "
                                        "{'a': [ { 'b': [], 'c': [] } ] }"
                                        )[0].oRecordData

    def testLinkList(self):

        DB = pyorient.OrientDB("localhost", 2424)
        DB.connect("root", "root")

        db_name = "test_tr"
        try:
            DB.db_drop(db_name)
        except pyorient.PyOrientCommandException as e:
            print(e)
            pass
        finally:
            db = DB.db_create(db_name, pyorient.DB_TYPE_GRAPH,
                                       pyorient.STORAGE_TYPE_MEMORY)
            pass

        DB.command( "insert into V set key1 = 'row0'" )
        DB.command( "insert into V set key1 = 'row1'" )
        DB.command( "insert into V set key1 = 'row2'" )
        DB.command( "insert into V set key1 = 'row3'" )

        o1 = pyorient.OrientRecordLink( "9:0" )
        o2 = pyorient.OrientRecordLink( "9:1" )
        o3 = pyorient.OrientRecordLink( "9:2" )
        o4 = pyorient.OrientRecordLink( "9:3" )
        lList = [ o1, o2, o3, o4 ]

        rec = DB.record_create( 9, { 'test': lList, 'key1': 'row4' } )  # 9:4

        if self.client.version.major > 1:
            _rec = DB.record_load( rec._rid )
            assert len( _rec.oRecordData['test'] ) == 4
            assert isinstance( _rec.oRecordData['test'][0], pyorient.OrientRecordLink )

