import unittest
from pyorient.serializations import OrientSerialization
from pyorient.otypes import OrientBinaryObject, OrientRecord


def test_mixed_list():
    rec = OrientRecord({
        '__o_class': 'ListTest',
        'list': [1, 'a']
    })

    serializer = OrientSerialization.get_impl(OrientSerialization.CSV)
    raw = serializer.encode(rec)
    assert raw == 'ListTest@list:[1,"a"]'


class SerializationTestCase(unittest.TestCase):

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

    def test_binary_link(self):
        from pyorient.otypes import OrientRecordLink
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
