import unittest
from pyorient.serializations import OrientSerialization
from pyorient.otypes import OrientBinaryObject, OrientRecord


class SerializationTestCase(unittest.TestCase):

    def test_binary_serializer(self):
        serializer = OrientSerialization.get_impl(OrientSerialization.Binary)
        content = 'Animal@name:"rat",specie:"rodent",out_Eat:%AQAAAAEADQAAAAAAAAAAAAAAAAAAAAAAAA==;'

        with self.assertRaises(NotImplementedError):
            serializer.decode(content)

        rec = OrientRecord({
            '__o_class': 'Animal',
            'name':'rat',
            'specie': 'rodent'
        })

        with self.assertRaises(NotImplementedError):
            serializer.encode(rec)


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
            'name':'rat',
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

        cluster_id = DB.command( "CREATE CLASS MyModel EXTENDS V" )[0]

        data0 = {'key': '"""'}
        DB.record_create( cluster_id, {'@MyModel': data0} )

        data1 = {'key': "'''"}
        DB.record_create( cluster_id, {'@MyModel': data1} )

        data2 = {'key': '\\'}
        DB.record_create( cluster_id, {'@MyModel': data2} )

        data3 = {'key': '\0'}
        DB.record_create( cluster_id, {'@MyModel': data3} )

        data4 = {'key': '""'}
        DB.record_create( cluster_id, {'@MyModel': data4} )

        data5 = {'key': '\'\'""\0 \\ execution'}
        DB.record_create( cluster_id, {'@MyModel': data5} )

        rec0 = DB.record_load( "#" + cluster_id.decode() + ":0" )
        assert rec0._class == "MyModel"
        assert rec0.oRecordData == data0

        rec1 = DB.record_load( "#" + cluster_id.decode('utf-8') + ":1" )
        assert rec1._class == "MyModel"
        assert rec1.oRecordData == data1

        rec2 = DB.record_load( "#" + cluster_id.decode('utf-8') + ":2")
        assert rec2._class == "MyModel"
        assert rec2.oRecordData == data2

        rec3 = DB.record_load( "#" + cluster_id.decode('utf-8') + ":3" )
        assert rec3._class == "MyModel"
        assert rec3.oRecordData == data3

        rec4 = DB.record_load( "#" + cluster_id.decode('utf-8') + ":4" )
        assert rec4._class == "MyModel"
        assert rec4.oRecordData == data4

        rec5 = DB.record_load( "#" + cluster_id.decode('utf-8') + ":5" )
        assert rec5._class == "MyModel"
        assert rec5.oRecordData == data5

