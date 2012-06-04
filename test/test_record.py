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

class RecordTestCase(unittest.TestCase):
    """ Command Test Case """
    ordb = None
    c = getTestConfig()
    dbname = c['existing_db']
    position = None
    dictToLoad = {
        "name":"Mario",
        "nan" : 23,
        "complexx" : "yoo",
        "array":["asd",1,2,3,"test sdf"]
    }
    
    def setUp(self):
        self.ordb = pyorient.OrientDB(self.c['host'], self.c['port'], self.c['useru'],self.c['userp'])
        self.ordb.dbopen(self.dbname)

    def tearDown(self):
        self.ordb.command("delete from #3:%s" % self.position)
        try:
            self.ordb.dbclose()
        except pyorient.PyOrientException, e:
            print("tryed close db but: (PyOrientException) %s" % e)
        self.ordb.close()

  
    def test_recordcreate(self):
        self.position = self.ordb.recordcreate(3, self.dictToLoad)
        print("\n\t pos %s" % self.position)
        orecord = self.ordb.recordload(3, self.position)

        self.assertNotEqual(self.position, -1,  "failed creating a record  %d" % self.position)
        #self.assertEqual(orecord.name, self.dictToLoad['name'], "retrieved bad record")

    def test_recordlaod(self):
        #self.position = self.ordb.recordcreate(3, self.dictToLoad)
        orecord = self.ordb.recordload(3, 2)
        print("\n\t Orecord name is writer? %s" % (orecord.name))
        self.assertEqual(orecord.name,'writer', "failed load a record %s" % orecord)

    
