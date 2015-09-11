import unittest
from pyorient.serializations import OrientSerialization
from pyorient.types import OrientBinaryObject, OrientRecord


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
