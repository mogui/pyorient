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

class SessionTestCase(unittest.TestCase):
    """ Command Test Case """
    ordb = None
    ordb = None
    c = getTestConfig()
    dbname = c['new_db']
    position = None
    
    def setUp(self):
        self.ordb = pyorient.OrientDB(self.c['host'], self.c['port'], self.c['rootu'],self.c['rootp'])
        
        if self.ordb.dbexists(self.dbname):
            print("exist delete")
            self.ordb.dbdelete(self.dbname)

        self.ordb.dbcreate(self.dbname)
        self.ordb.dbopen(self.dbname)
   
    def tearDown(self):
        if self.ordb.dbexists(self.dbname):
            print("cleaning up destroying db")
            self.ordb.dbdelete(self.dbname)
        self.ordb.dbclose(self.dbname)
        self.ordb.close()
        pass

  
    def test_full_session(self):
        #self.ordb.dbopen()
        print("\n--------------------------------")
        output("Db size is %d" % self.ordb.dbsize())
        output("creating cluster...")
        clid = self.ordb.dataclusteradd(pyorient.CLUSTER_MEMORY, "clusterx", "clusterx")
        output("created cluster id: %d" % clid)
        output("Creting a class and defining a property...")
        self.ordb.command("Create Class Cars CLUSTER 6")
        self.ordb.command("Create Property Cars.modello string")
        output("Creating a record in the class")
        record = pyorient.OrientRecord({
                'modello':"Audi",
                'propietario':{'name':'Niko', 'surname':'Usai'},
                'quantity':34
            }, o_class='Cars')
        position = self.ordb.recordcreate(6, record)
        output("created record with position: %d" % position)
        record.modello = 'Fiat'
        record.quantity = 12
        position = self.ordb.recordcreate(6, record)
        output("created another record record with position: %d" % position)
        output("making a query...")
        result = self.ordb.command("select from Cars")
        for rec in result:
            output("\t query result -> %s" % rec)

        print("\n--------------------------------")


def output(string):
    print("| \t> %s " % string)
        