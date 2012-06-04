from __future__ import print_function

#   Copyright 2012 Niko Usai <usai.niko@gmail.com>, http://mogui.it
#
#   this file is part of pyorient
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import pyorient
import unittest
import time
from . import getTestConfig

class DataclusterTestCase(unittest.TestCase):
    """ Data cluster test case """
    ordb = None
    c = getTestConfig()
    dbname = c['existing_db']
    clid = None
    
    def setUp(self):
        self.ordb = pyorient.OrientDB(self.c['host'], self.c['port'], self.c['useru'],self.c['userp'])
        self.ordb.dbopen(self.dbname)
        
    def tearDown(self):
        self.clid = None
        try:
            self.ordb.dbclose()
        except pyorient.PyOrientException, e:
            print("tryed close db but: (PyOrientException) %s" % e)
        self.ordb.close()

    def test_dataclusteradd_remove(self):
        clid = self.ordb.dataclusteradd(pyorient.CLUSTER_PHYSICAL, "testcluster", "testcluster")
        self.assertTrue(clid >= 0, "No cluster created %d " % clid)
        print("\n\tcluster created clid: %d" % clid)
        if clid >=0:
            delret = self.ordb.dataclusterremove(clid)
        self.assertTrue(delret, "Not deleted cluster, ret:%d" % delret)

    def test_dataclusterremove_wrong_clid(self):
        delret = self.ordb.dataclusterremove(123123)
        self.assertFalse(delret, "dataclusteradd returned true with wrong clid, ret:%d" % delret)
        
    def test_dataclustercount(self):
        count = self.ordb.dataclustercount(3)
        print("\n\t count: %ld" % count)
        self.assertTrue(count >= 0, "data cluster count is negative: %d" % count)
        
    def test_dataclusterdatarange(self):
        (begin, end) = self.ordb.dataclusterdatarange(3)
        print("\n\t range is %ld %ld" % (begin, end))
        self.assertTrue(end >= begin, "data range isn't well formed: %ld %ld" % (begin, end))