__author__ = 'Ostico <ostico@gmail.com>'

import os
import sys
import unittest


from pyorient.constants import CLUSTER_TYPE_PHYSICAL
from pyorient.messages.database import DbOpenMessage, DbReloadMessage
from pyorient.messages.cluster import DataClusterDataRangeMessage, \
    DataClusterDropMessage, DataClusterAddMessage
from pyorient import OrientSocket


os.environ['DEBUG'] = "1"
os.environ['DEBUG_VERBOSE'] = "0"
if os.path.realpath('../') not in sys.path:
    sys.path.insert(0, os.path.realpath('../'))

if os.path.realpath('.') not in sys.path:
    sys.path.insert(0, os.path.realpath('.'))



# from pyorient.Messages.Constants.OrientPrimitives import *
# from pyorient.Messages.OrientSocket import OrientSocket
# from pyorient.Messages.Server.DbOpenMessage import DbOpenMessage
# from pyorient.Messages.Server.DbReloadMessage import DbReloadMessage

# from pyorient.Messages.Database.DataClusterDataRangeMessage import DataClusterDataRangeMessage
# from pyorient.Messages.Database.DataClusterAddMessage import DataClusterAddMessage
# from pyorient.Messages.Database.DataClusterDropMessage import DataClusterDropMessage
# from ORecordCoder import *


class DataClusterTestCase(unittest.TestCase):
    """ Command Test Case """

    def test_data_cluster_add_drop(self):
        import random

        connection = OrientSocket('localhost', 2424)

        db_name = 'GratefulDeadConcerts'
        db_open = DbOpenMessage( connection )
        clusters = db_open.prepare( ( db_name, 'admin', 'admin' ) ) \
            .send().fetch_response()

        _created_clusters = []
        for _rng in range(0, 5):
            data_cadd = DataClusterAddMessage( connection )
            new_cluster_id = data_cadd.prepare(
                [
                    'my_cluster_' + str( random.randint( 0, 999999999 ) ),
                    CLUSTER_TYPE_PHYSICAL    # 'PHYSICAL'
                ]
            ).send().fetch_response()
            _created_clusters.append( new_cluster_id )
            print("New cluster ID: %u" % new_cluster_id)

        os.environ['DEBUG'] = '0'  # silence debug

        _reload = DbReloadMessage(connection)
        new_cluster_list =_reload.prepare().send().fetch_response()

        new_cluster_list.sort(key=lambda cluster: cluster['id'])

        _list = []
        for cluster in new_cluster_list:
            datarange = DataClusterDataRangeMessage(connection)
            value = datarange.prepare(cluster['id']).send().fetch_response()
            print("Cluster Name: %s, ID: %u: %s " \
                  % ( cluster['name'], cluster['id'], value ))
            _list.append( cluster['id'] )
            assert value is not []
            assert value is not None

        # check for new cluster in database
        try:
            for _cl in _created_clusters:
                _list.index( _cl )
                print("New cluster found in reload.")
                assert True
        except ValueError:
            assert False

        # now drop all and repeat check
        for _cid in _created_clusters:
            drop_c = DataClusterDropMessage( connection )
            print("Drop cluster %u" % _cid)
            res = drop_c.prepare( _cid ).send().fetch_response()
            if res is True:
                print("Done")
            else:
                raise Exception( "Cluster " + str(_cid) + " failed")

        _reload = DbReloadMessage(connection)
        new_cluster_list = _reload.prepare().send().fetch_response()
        new_cluster_list.sort(key=lambda cluster: cluster['id'])

        _list = []
        for cluster in new_cluster_list:
            datarange = DataClusterDataRangeMessage(connection)
            value = datarange.prepare(cluster['id']).send().fetch_response()
            print("Cluster Name: %s, ID: %u: %s " \
                  % ( cluster['name'], cluster['id'], value ))
            _list.append( cluster['id'] )
            assert value is not []
            assert value is not None

        # check for removed cluster in database
        for _cl in _created_clusters:

            try:
                _list.index( _cl )
                assert False
            except ValueError:
                assert True
                print("Cluster %u deleted." % _cl)