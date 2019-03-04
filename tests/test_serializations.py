import unittest
from pyorient.serializations import OrientSerialization, CSVRidBagDecoder
from pyorient.otypes import OrientBinaryObject, OrientRecord, OrientRecordLink


def binary_db_connect():
    import pyorient
    DB = pyorient.OrientDB("localhost", 2424, OrientSerialization.Binary)
    DB.connect("root", "root")

    db_name = "binary_test"
    try:
        DB.db_drop(db_name)
    except pyorient.PyOrientCommandException as e:
        print(e)
    finally:
        db = DB.db_create(db_name, pyorient.DB_TYPE_GRAPH,
                          pyorient.STORAGE_TYPE_MEMORY)
        pass

    cluster_info = DB.db_open(
        db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
    )

    return DB


def skip_binary_if_pyorient_native_not_installed( func ):
    from pyorient.serializations import binary_support
    from os import sys
    import types

    if sys.version_info[ 0 ] < 3:
        test_instance = isinstance( func, ( type, types.ClassType ) )
    else:
        test_instance = isinstance( func, type )
    if not test_instance:
        if not binary_support:
            func.__unittest_skip__ = True
            func.__unittest_skip_why__ = "pyorient_native not installed."
    return func


class SerializationTestCase(unittest.TestCase):

    def test_mixed_list(self):
        rec = OrientRecord( {
            '__o_class': 'ListTest',
            'list': [ 1, 'a' ]
        } )

        serializer = OrientSerialization.get_impl( OrientSerialization.CSV )
        raw = serializer.encode( rec )
        assert raw == 'ListTest@list:[1,"a"]'

    @skip_binary_if_pyorient_native_not_installed
    def test_binary_string(self):
        DB = binary_db_connect()
        DB.command("CREATE CLASS MyModel EXTENDS V")[0]

        cluster_id = DB.command("select classes[name='MyModel']" + \
                                ".defaultClusterId from 0:1")[0].oRecordData['classes']
        data = {'key': 'val'}
        DB.record_create(cluster_id, {'@MyModel': data})

        import sys
        if sys.version_info[0] >= 3 and isinstance(cluster_id, bytes):
            _n_rid = cluster_id.decode()
        else:
            _n_rid = str(cluster_id)

        rec = DB.record_load("#" + _n_rid + ":0")
        assert rec.oRecordData == data

    @skip_binary_if_pyorient_native_not_installed
    def test_binary_int(self):
        DB = binary_db_connect()
        DB.command("CREATE CLASS MyModel EXTENDS V")[0]
        cluster_id = DB.command("select classes[name='MyModel']" + \
                                ".defaultClusterId from 0:1")[0].oRecordData['classes']
        data = {'key': int(-1)}
        DB.record_create(cluster_id, {'@MyModel': data})

        import sys
        if sys.version_info[0] >= 3 and isinstance(cluster_id, bytes):
            _n_rid = cluster_id.decode()
        else:
            _n_rid = str(cluster_id)

        rec = DB.record_load("#" + _n_rid + ":0")
        assert rec.oRecordData == data

    @skip_binary_if_pyorient_native_not_installed
    def test_binary_long(self):
        DB = binary_db_connect()
        DB.command("CREATE CLASS MyModel EXTENDS V")[0]
        cluster_id = DB.command("select classes[name='MyModel']" + \
                                ".defaultClusterId from 0:1")[0].oRecordData['classes']
        import sys
        if sys.version_info[0] < 3:
            data = {'key': long(-1)}
        else:
            data = {'key': int(-1)}
        DB.record_create(cluster_id, {'@MyModel': data})

        if sys.version_info[0] >= 3 and isinstance(cluster_id, bytes):
            _n_rid = cluster_id.decode()
        else:
            _n_rid = str(cluster_id)
        rec = DB.record_load("#" + _n_rid + ":0")
        assert rec.oRecordData == data

    @skip_binary_if_pyorient_native_not_installed
    def test_binary_float(self):
        DB = binary_db_connect()
        DB.command("CREATE CLASS MyModel EXTENDS V")[0]
        cluster_id = DB.command("select classes[name='MyModel']" + \
                                ".defaultClusterId from 0:1")[0].oRecordData['classes']
        data = {'key': 1.0}
        DB.record_create(cluster_id, {'@MyModel': data})

        import sys
        if sys.version_info[0] >= 3 and isinstance(cluster_id, bytes):
            _n_rid = cluster_id.decode()
        else:
            _n_rid = str(cluster_id)

        rec = DB.record_load("#" + _n_rid + ":0")
        assert rec.oRecordData == data

    @skip_binary_if_pyorient_native_not_installed
    def test_binary_list(self):
        DB = binary_db_connect()
        DB.command("CREATE CLASS MyModel EXTENDS V")[0]
        cluster_id = DB.command("select classes[name='MyModel']" + \
                                ".defaultClusterId from 0:1")[0].oRecordData['classes']

        data = {'key': [1, 'a', 3, 4.0, [42, 27]]}
        DB.record_create(cluster_id, {'@MyModel': data})

        import sys
        if sys.version_info[0] >= 3 and isinstance(cluster_id, bytes):
            _n_rid = cluster_id.decode()
        else:
            _n_rid = str(cluster_id)

        rec = DB.record_load("#" + _n_rid + ":0")
        assert rec.oRecordData == data

    @skip_binary_if_pyorient_native_not_installed
    def test_binary_dict(self):
        DB = binary_db_connect()
        DB.command("CREATE CLASS MyModel EXTENDS V")[0]
        cluster_id = DB.command("select classes[name='MyModel']" + \
                                ".defaultClusterId from 0:1")[0].oRecordData['classes']

        data = {'key': {'str': 'a', 'int': 0, 'list': [1, 2, 3], 'nested_dict':
            {'nestkey': 'nestval'}}}
        DB.record_create(cluster_id, {'@MyModel': data})

        import sys
        if sys.version_info[0] >= 3 and isinstance(cluster_id, bytes):
            _n_rid = cluster_id.decode()
        else:
            _n_rid = str(cluster_id)

        rec = DB.record_load("#" + _n_rid + ":0")
        assert rec.oRecordData == data

    @skip_binary_if_pyorient_native_not_installed
    def test_binary_link(self):
        DB = binary_db_connect()
        DB.command("CREATE CLASS MyModel EXTENDS V")
        cluster_id = DB.command("select classes[name='MyModel']" + \
                                ".defaultClusterId from 0:1")[0].oRecordData['classes']

        data = {'key': 'node0'}
        DB.record_create(cluster_id, {'@MyModel': data})

        data1 = {'key': 'node1'}
        DB.record_create(cluster_id, {'@MyModel': data1})

        import sys
        if sys.version_info[0] >= 3 and isinstance(cluster_id, bytes):
            _n_rid = cluster_id.decode()
        else:
            _n_rid = str(cluster_id)

        DB.command("CREATE CLASS test EXTENDS E")
        DB.command("create edge test from %s to %s" % ("#" + _n_rid + ":0",
                                                       "#" + _n_rid + ":1"))
        rec0 = DB.record_load("#" + _n_rid + ":0")
        rec1 = DB.record_load("#" + _n_rid + ":1")
        link = DB.record_load(rec0.oRecordData['out_test'][0].get_hash())
        assert link.oRecordData['out'].get_hash() == rec0._rid
        assert link.oRecordData['in'].get_hash() == rec1._rid
        assert rec0.oRecordData['out_test'][0].get_hash() == \
               rec1.oRecordData['in_test'][0].get_hash()

    @skip_binary_if_pyorient_native_not_installed
    def test_binary_date(self):
        import datetime
        DB = binary_db_connect()
        DB.command("CREATE CLASS MyModel EXTENDS V")[0]
        cluster_id = DB.command("select classes[name='MyModel']" + \
                                ".defaultClusterId from 0:1")[0].oRecordData['classes']

        data = {'key': datetime.date.today()}
        DB.record_create(cluster_id, {'@MyModel': data})

        import sys
        if sys.version_info[0] >= 3 and isinstance(cluster_id, bytes):
            _n_rid = cluster_id.decode()
        else:
            _n_rid = str(cluster_id)

        rec = DB.record_load("#" + _n_rid + ":0")
        assert rec.oRecordData == data

    @skip_binary_if_pyorient_native_not_installed
    def test_binary_datetime(self):
        import datetime
        DB = binary_db_connect()
        DB.command("CREATE CLASS MyModel EXTENDS V")[0]
        cluster_id = DB.command("select classes[name='MyModel']" + \
                                ".defaultClusterId from 0:1")[0].oRecordData['classes']

        dt = datetime.datetime.now()
        # OrientDB datetime has millisecond precision
        dt = dt.replace(microsecond=int(dt.microsecond / 1000) * 1000)
        data = {'key': dt}
        DB.record_create(cluster_id, {'@MyModel': data})

        import sys
        if sys.version_info[0] >= 3 and isinstance(cluster_id, bytes):
            _n_rid = cluster_id.decode()
        else:
            _n_rid = str(cluster_id)

        rec = DB.record_load("#" + _n_rid + ":0")
        assert rec.oRecordData == data

    @skip_binary_if_pyorient_native_not_installed
    def test_binary_utc(self):
        serializer = OrientSerialization.get_impl(OrientSerialization.Binary)
        utc_serializer = OrientSerialization.get_impl(OrientSerialization.Binary, {'utc':True})

        from datetime import date, datetime
        dates = OrientRecord({
            '__o_class': 'dates',
            'd': date(2017, 7, 9),
            'dt': datetime(2017, 7, 9, 12, 34, 56),
        })

        local_serialized = serializer.encode(dates)
        utc_serialized = utc_serializer.encode(dates)
        if not datetime.now().replace(second=0, microsecond=0) == datetime.utcnow().replace(second=0, microsecond=0):
            self.assertNotEqual(local_serialized, utc_serialized)

        # TODO Figure out how to test these reliably
        #self.assertIn(b'\x02d\x00\x00\x00\x17\x13', utc_serialized) 
        #self.assertIn(b'\x04dt\x00\x00\x00\x1a\x06\x00\x98\x8f\x02\x80', utc_serialized) 

        local_deserialized = serializer.decode(local_serialized)
        self.assertDictEqual(dates.oRecordData, local_deserialized[1])
        utc_deserialized = utc_serializer.decode(utc_serialized)
        self.assertDictEqual(dates.oRecordData, utc_deserialized[1])

    @skip_binary_if_pyorient_native_not_installed
    def test_binary_none(self):
        DB = binary_db_connect()
        DB.command("CREATE CLASS MyModel EXTENDS V")[0]
        cluster_id = DB.command("select classes[name='MyModel']" + \
                                ".defaultClusterId from 0:1")[0].oRecordData['classes']

        data = {'key': None}
        DB.record_create(cluster_id, {'@MyModel': data})

        import sys
        if sys.version_info[0] >= 3 and isinstance(cluster_id, bytes):
            _n_rid = cluster_id.decode()
        else:
            _n_rid = str(cluster_id)

        rec = DB.record_load("#" + _n_rid + ":0")
        assert rec.oRecordData == data

    def test_csv_decoding(self):
        serializer = OrientSerialization.get_impl(OrientSerialization.CSV)
        content = 'Animal@name:"rat",specie:"rodent",out_Eat:%AQAAAAEADQAAAAAAAAAAAAAAAAAAAAAAAA==;'
        _, record = serializer.decode(content)

        assert isinstance(record, dict)
        assert record['name'] == 'rat'
        assert isinstance(record['out_Eat'], OrientBinaryObject)

        eat_decoder = CSVRidBagDecoder(record['out_Eat'].getBin())
        eat = next(eat_decoder.decode_embedded())
        self.assertIsInstance(eat, OrientRecordLink)
        self.assertEqual(eat.get_hash(), '#13:0')

        # TODO: add several more complex tests to have more coverage

    def test_csv_encoding(self):
        rec = OrientRecord({
            '__o_class': 'Animal',
            'name': 'rat',
            'specie': 'rodent'
        })
        serializer = OrientSerialization.get_impl(OrientSerialization.CSV)
        raw = serializer.encode(rec)
        assert raw.startswith('Animal@')
        assert 'name:"rat"' in raw
        assert 'specie:"rodent"' in raw

        # TODO: add several more complex tests to have more coverage

    def test_csv_escape(self):
        import pyorient
        DB = pyorient.OrientDB("localhost", 2424)
        DB.connect("root", "root")

        db_name = "test_escape"
        try:
            DB.db_drop(db_name)
        except pyorient.PyOrientCommandException as e:
            print(e)
        finally:
            db = DB.db_create(db_name, pyorient.DB_TYPE_GRAPH,
                              pyorient.STORAGE_TYPE_MEMORY)
            pass

        cluster_info = DB.db_open(
            db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
        )

        cluster_id = DB.command("CREATE CLASS MyModel EXTENDS V")[0]

        data0 = {'key': '"""'}
        DB.record_create(cluster_id, {'@MyModel': data0})

        data1 = {'key': "'''"}
        DB.record_create(cluster_id, {'@MyModel': data1})

        data2 = {'key': '\\'}
        DB.record_create(cluster_id, {'@MyModel': data2})

        data3 = {'key': '\0'}
        DB.record_create(cluster_id, {'@MyModel': data3})

        data4 = {'key': '""'}
        DB.record_create(cluster_id, {'@MyModel': data4})

        data5 = {'key': '\'\'""\0 \\ execution'}
        DB.record_create(cluster_id, {'@MyModel': data5})

        import sys
        if sys.version_info[0] >= 3 and isinstance(cluster_id, bytes):
            _n_rid = cluster_id.decode()
        else:
            _n_rid = str(cluster_id)

        rec0 = DB.record_load("#" + _n_rid + ":0")
        # assert rec0._class == "MyModel"
        assert rec0.oRecordData == data0

        rec1 = DB.record_load("#" + _n_rid + ":1")
        # assert rec1._class == "MyModel"
        assert rec1.oRecordData == data1

        rec2 = DB.record_load("#" + _n_rid + ":2")
        # assert rec2._class == "MyModel"
        assert rec2.oRecordData == data2

        rec3 = DB.record_load("#" + _n_rid + ":3")
        # assert rec3._class == "MyModel"
        assert rec3.oRecordData == data3

        rec4 = DB.record_load("#" + _n_rid + ":4")
        # assert rec4._class == "MyModel"
        assert rec4.oRecordData == data4

        rec5 = DB.record_load("#" + _n_rid + ":5")
        # assert rec5._class == "MyModel"
        assert rec5.oRecordData == data5

    def test_csv_utc(self):
        serializer = OrientSerialization.get_impl(OrientSerialization.CSV)
        utc_serializer = OrientSerialization.get_impl(OrientSerialization.CSV, {'utc':True})

        from datetime import date, datetime
        dates = OrientRecord({
            '__o_class': 'dates',
            'd': date(2017, 7, 9),
            'dt': datetime(2017, 7, 9, 12, 34, 56),
        })

        local_serialized = serializer.encode(dates)
        utc_serialized = utc_serializer.encode(dates)
        if not datetime.now().replace(second=0, microsecond=0) == datetime.utcnow().replace(second=0, microsecond=0):
            self.assertNotEqual(local_serialized, utc_serialized)

        self.assertIn('d:1499558400000a', utc_serialized)
        self.assertIn('dt:1499603696000t', utc_serialized)

        local_deserialized = serializer.decode(local_serialized)
        self.assertDictEqual(dates.oRecordData, local_deserialized[1])
        utc_deserialized = utc_serializer.decode(utc_serialized)
        self.assertDictEqual(dates.oRecordData, utc_deserialized[1])

