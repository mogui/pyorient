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

        self.cluster_info = self.client.db_open(
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
        assert len(res[0].oRecordData['link']) == 4
        for link in res[0].oRecordData['link']:
            assert link.clusterID == '11', "Failed to assert that 11 equals " + \
                                           link.clusterID

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

        x = self.client.query("SELECT embMap.keys() FROM #13:0")[0].oRecordData

        assert 'embMap' in x
        assert 'it' in x['embMap']
        assert 'en' in x['embMap']
        assert 'ru' in x['embMap']

        x = self.client.query(
            "SELECT embMap.keys().asString() FROM #13:0"
        )[0].oRecordData

        assert 'embMap' in x
        assert x['embMap'] == '[it, en, ru]'

    def testEmbedNum(self):

        self._links_cluster_id = self.client.command("create class test_embed "
                                                     "extends V")[0]

        self.client.command("create vertex test_embed "
                            "set embMap = "
                            "{'en': 1,'it':2, 'ru':3}")

        x = self.client.query("SELECT embMap.values() FROM #13:0")[
            0].oRecordData

        assert 'embMap' in x
        assert 1 in x['embMap']
        assert 2 in x['embMap']
        assert 3 in x['embMap']

        x = self.client.query(
            "SELECT embMap.values().asString() FROM #13:0"
        )[0].oRecordData

        assert 'embMap' in x
        assert x['embMap'] == '[2, 1, 3]'

    def testEmbeddedMapsInList(self):
        class_id1 = self.client.command("CREATE VERTEX V SET mapInList = "
                                        "[ {'one': 2, 'three': 4 } ]"
                                        )[0].oRecordData

        assert 'mapInList' in class_id1
        assert len(class_id1[ 'mapInList' ]) == 1
        assert class_id1[ 'mapInList' ][0]['one'] == 2
        assert class_id1[ 'mapInList' ][0]['three'] == 4
