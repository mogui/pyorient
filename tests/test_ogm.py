import sys
import unittest
import decimal
import os.path
import timeit
from datetime import datetime

from pyorient import PyOrientCommandException, PyOrientSQLParsingException
from pyorient.ogm import Graph, Config
from pyorient.groovy import GroovyScripts

from pyorient.ogm.declarative import declarative_node, declarative_relationship
from pyorient.ogm.property import (
    Boolean, String, Date, DateTime, Float, Decimal, Double, Integer, Short,
    Long, EmbeddedMap, EmbeddedSet, Link, LinkList, UUID)
from pyorient.ogm.what import expand, in_, out, outV, inV, distinct, sysdate, QV, unionall

from pyorient.ogm.update import Update
from pyorient.ogm.sequence import Sequence, NewSequence, sequence

AnimalsNode = declarative_node()
AnimalsRelationship = declarative_relationship()

class Animal(AnimalsNode):
    element_type = 'animal'
    element_plural = 'animals'

    name = String(nullable=False, unique=True)
    specie = String(nullable=False)

class Food(AnimalsNode):
    element_type = 'food'
    element_plural = 'foods'

    name = String(nullable=False, unique=True)
    color = String(nullable=False)

class Beverage(AnimalsNode):
    element_type = 'beverage'
    element_plural = 'beverages'

    name = String(nullable=False, unique=True)
    color = String(nullable=False)

class Eats(AnimalsRelationship):
    label = 'eats'
    modifier = String()

class Dislikes(AnimalsRelationship):
    label = 'dislikes'

class Drinks(AnimalsRelationship):
    label = 'drinks'
    modifier = String()

class OGMAnimalsTestCaseBase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(OGMAnimalsTestCaseBase, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('animals', 'root', 'root',
                                           initial_drop=True))


        g.create_all(AnimalsNode.registry)
        g.create_all(AnimalsRelationship.registry)

    def testGraph(self):
        assert len(AnimalsNode.registry) == 3
        assert len(AnimalsRelationship.registry) == 3

        g = self.g

        rat = g.animals.create(name='rat', specie='rodent')
        mouse = g.animals.create(name='mouse', specie='rodent')
        queried_rat = g.query(Animal).filter(
            Animal.name.endswith('at') | (Animal.name == 'tiger')).one()

        assert rat == queried_rat

        invalid_query_args = {'name': 'rat', 'name="rat" OR 1': 1}
        try:
            g.animals.query(**invalid_query_args).all()
        except:
            pass
        else:
            assert False and 'Invalid params did not raise an exception!'

        queried_mouse = g.query(mouse).one()
        assert mouse == queried_mouse
        assert mouse == g.get_vertex(mouse._id)
        assert mouse == g.get_element(mouse._id)

        try:
            rat2 = g.animals.create(name='rat', specie='rodent')
        except:
            pass
        else:
            assert False and 'Uniqueness not enforced correctly'

        pea = g.foods.create(name='pea', color='green')
        queried_pea = g.foods.query(color='green', name='pea').one()

        cheese = g.foods.create(name='cheese', color='yellow')

        assert queried_pea == pea

        rat_eats_pea = g.eats.create(queried_rat, queried_pea, modifier='lots')
        mouse_eats_pea = g.eats.create(mouse, pea)
        mouse_eats_cheese = Eats.objects.create(mouse, cheese)

        assert rat_eats_pea.modifier == 'lots'
        assert rat_eats_pea == g.get_edge(rat_eats_pea._id)
        assert rat_eats_pea == g.get_element(rat_eats_pea._id)

        water = g.beverages.create(name='water', color='clear')
        mouse_drinks_water = g.drinks.create(mouse, water)

        assert [water] == mouse.out(Drinks)
        assert [mouse_drinks_water] == mouse.outE(Drinks)
        assert [water] == mouse.both(Drinks)
        assert [mouse_drinks_water] == mouse.bothE(Drinks)

        nut = g.foods.create(name='nut', color='brown')
        rat_dislikes_nut = g.dislikes.create(rat, nut)
        mouse_eats_nut = g.eats.create(mouse, nut)

        assert [rat] == nut.in_(Dislikes)
        assert [rat_dislikes_nut] == nut.inE(Dislikes)

        eaters = g.in_(Food, Eats)
        assert rat in eaters

        # Who eats the peas?
        pea_eaters = g.foods.query(name='pea').what(expand(in_(Eats)))
        for animal in pea_eaters:
            print(animal.name, animal.specie)

        # Which animals eat each food
        # FIXME Currently calling all() here, as iteration over expand()
        # results is currently broken.
        animal_foods = \
            g.animals.query().what(expand(distinct(out(Eats)))).all()
        for food in animal_foods:
            print(food.name, food.color,
                  g.query(
                      g.foods.query(name=food.name).what(expand(in_(Eats)))) \
                             .what(Animal.name).all())

        for food_name, food_color in g.query(Food.name, Food.color):
            print(food_name, food_color) # 'pea green' # 'cheese yellow'

        # FIXME While it is nicer to use files, parser should be more
        # permissive with whitespace
        g.scripts.add(GroovyScripts.from_string(
"""
def get_eaters_of(food_type) {
    return g.V('@class', 'food').has('name', T.eq, food_type).inE().outV();
}

def get_foods_eaten_by(animal) {
    return g.v(animal).outE('eats').inV()
}

def get_colored_eaten_foods(animal, color) {
    return g.v(animal).outE('eats').inV().has('color', T.eq, color)
}
"""))

        pea_eaters = g.gremlin('get_eaters_of', 'pea')
        for animal in pea_eaters:
            print(animal.name, animal.specie) # 'rat rodent' # 'mouse rodent'

        rat_cuisine = g.gremlin('get_foods_eaten_by', (rat,))
        for food in rat_cuisine:
            print(food.name, food.color) # 'pea green'

        batch = g.batch()
        batch['zombie'] = batch.animals.create(name='zombie',specie='undead')
        batch['brains'] = batch.foods.create(name='brains', color='grey')

        # Retry up to twenty times
        batch[:] = batch.eats.create(batch[:'zombie'], batch[:'brains']).retry(20)

        with batch.if_((batch[:'brains'].in_(Eats).size() == 1) | (batch[:'brains'].color == 'grey')):
            batch['white_matter'] = batch.foods.create(name='delicacy brains', color='white')
            batch[:] = batch.eats.create(batch[:'zombie'], batch[:'white_matter'])

        batch['unicorn'] = batch.animals.create(name='unicorn', specie='mythical')
        batch['unknown'] = batch.foods.create(name='unknown', color='rainbow')
        batch['mystery_diet'] = batch[:'unicorn'](Eats) > batch[:'unknown']

        # Commits and clears batch
        zombie = batch['$zombie']
        assert zombie.specie == 'undead'


class OGMAnimalsRegistryTestCase(OGMAnimalsTestCaseBase):
    def testRegistry(self):
        g = self.g
        schema_registry = g.build_mapping(declarative_node(), declarative_relationship(), auto_plural=True)
        assert all(c in schema_registry for c in ['animal', 'food', 'eats'])

        assert type(schema_registry['animal'].specie) == String

        # Plurals not communicated to schema; postprocess registry before
        # include() if you have a better solution than auto_plural.
        assert schema_registry['food'].registry_plural != Food.registry_plural
        g.clear_registry()
        assert len(g.registry) == 0
        g.include(schema_registry)

        assert set(g.registry.keys()) == set(['food', 'dislikes', 'eats', 'beverage', 'animal', 'drinks'])

        rat = g.animal.create(name='rat', specie='rodent')
        mouse = g.animal.create(name='mouse', specie='rodent')
        rat_class = g.registry['animal']
        queried_rat = g.query(rat_class).filter(
            rat_class.name.endswith('at') | (rat_class.name == 'tiger')).one()

        assert rat == queried_rat

        # try again, to make sure that brokers get cleared correctly
        schema_registry = g.build_mapping(
            declarative_node(), declarative_relationship(), auto_plural=True)
        g.clear_registry()
        g.include(schema_registry)
        assert set(g.registry.keys()) == set(['food', 'dislikes', 'eats', 'beverage', 'animal', 'drinks'])


MoneyNode = declarative_node()
MoneyRelationship = declarative_relationship()

class Person(MoneyNode):
    element_plural = 'people'

    full_name = String(nullable=False)
    uuid = String(nullable=False, default=UUID())

class Wallet(MoneyNode):
    element_plural = 'wallets'

    amount_precise = Decimal(name='amount', nullable=False)
    amount_imprecise = Double()

class Carries(MoneyRelationship):
    # No label set on relationship; Broker will not be attached to graph.
    pass

class OGMMoneyTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(OGMMoneyTestCase, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('money', 'root', 'root'
                                           , initial_drop=True))

        g.create_all(MoneyNode.registry)
        g.create_all(MoneyRelationship.registry)

    def testDoubleSerialization(self):
        # Using str() on a float object in Python 2 sometimes
        # returns scientific notation, which causes queries to be misapplied.
        # Similarly, many alternative approaches of turning floats to strings
        # in Python can cause loss of precision.
        g = self.g

        # Try very large values, very small values, and values with a lot of decimals.
        target_values = [1e50, 1e-50, 1.23456789012]

        for value in target_values:
            amount_imprecise = value
            amount_precise = decimal.Decimal(amount_imprecise)

            original_wallet = g.wallets.create(amount_imprecise=amount_imprecise,
                                               amount_precise=amount_precise)
            wallet = g.query(Wallet).filter(
                (Wallet.amount_imprecise > (value * (1 - 1e-6))) &
                (Wallet.amount_imprecise < (value * (1 + 1e+6)))
            ).one()

            assert wallet.amount_imprecise == original_wallet.amount_imprecise
            assert wallet.amount_precise == original_wallet.amount_precise

    def testMoney(self):
        assert len(MoneyNode.registry) == 2
        assert len(MoneyRelationship.registry) == 1

        g = self.g

        if g.server_version.major == 1:
            self.skipTest(
                'UUID method does not exists in OrientDB version < 2')

        costanzo = g.people.create(full_name='Costanzo Veronesi', uuid=UUID())
        valerius = g.people.create(full_name='Valerius Burgstaller'
                                   , uuid=UUID())
        if g.server_version >= (2,1,0):
            # Default values supported
            oliver = g.people.create(full_name='Oliver Girard')
        else:
            oliver = g.people.create(full_name='Oliver Girard', uuid=UUID())

        # If you override nullable properties to be not-mandatory, be aware that
        # OrientDB version < 2.1.0 does not count null
        assert Person.objects.query().what(distinct(Person.uuid)).count() == 3

        original_inheritance = decimal.Decimal('1520841.74309871919')

        inheritance = g.wallets.create(
            amount_precise = original_inheritance
            , amount_imprecise = original_inheritance)

        assert inheritance.amount_precise == original_inheritance
        assert inheritance.amount_precise != inheritance.amount_imprecise

        pittance = decimal.Decimal('0.1')
        poor_pouch = g.wallets.create(
            amount_precise=pittance
            , amount_imprecise=pittance)

        assert poor_pouch.amount_precise == pittance
        assert poor_pouch.amount_precise != poor_pouch.amount_imprecise

        # Django-style creation
        costanzo_claim = Carries.objects.create(costanzo, inheritance)
        valerius_claim = Carries.objects.create(valerius, inheritance)
        oliver_carries = Carries.objects.create(oliver, poor_pouch)

        g.scripts.add(GroovyScripts.from_file(
            os.path.join(
                os.path.split(
                    os.path.abspath(__file__))[0], 'money.groovy')), 'money')
        rich_list = g.gremlin('rich_list', 1000000, namespace='money')
        assert costanzo in rich_list and valerius in rich_list \
            and oliver not in rich_list

        bigwallet_query = g.query(Wallet).filter(Wallet.amount_precise > 100000)
        smallerwallet_query = g.query(Wallet).filter(
            Wallet.amount_precise < 100000)

        # Basic query slicing
        assert len(bigwallet_query[:]) == 1
        assert len(smallerwallet_query) == 1

        assert bigwallet_query.first() == inheritance

        pouch = smallerwallet_query[0]
        assert pouch == poor_pouch

        assert len(pouch.outE()) == len(pouch.out())
        assert pouch.in_() == pouch.both() and pouch.inE() == pouch.bothE()

        first_inE = pouch.inE()[0]
        assert first_inE == oliver_carries
        assert first_inE.outV() == oliver and first_inE.inV() == poor_pouch

        for i, wallet in enumerate(g.query(Wallet)):
            print(decimal.Decimal(wallet.amount_imprecise) -
                    wallet.amount_precise)
            assert i < 2


        schema_registry = g.build_mapping(MoneyNode, MoneyRelationship)
        assert all(c in schema_registry for c in ['person', 'wallet', 'carries'])

        WalletType = schema_registry['wallet']

        # Original property name, amount_precise, lost-in-translation
        assert type(WalletType.amount) == Decimal
        assert type(WalletType.amount_imprecise) == Double
        g.include(schema_registry)

        debt = decimal.Decimal(-42.0)
        WalletType.objects.create(amount=debt, amount_imprecise=0)

        assert g.query(Wallet)[2].amount == -42

class OGMClassTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(OGMClassTestCase, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('classes', 'root', 'root'
                                           , initial_drop=True))

    def testGraph(self):
        g = self.g

        try:
            # The WRONG way to do multiple inheritance
            # Here, Foo.registry and Bar.registry reference different classes,
            # and therefore g.create_all() can not work.
            class Foo(declarative_node()):
                pass

            class Bar(declarative_node()):
                pass

            class Fubar(Foo, Bar):
                pass
        except TypeError:
            pass
        else:
            assert False and 'Failed to enforce correct vertex base classes.'


DateTimeNode = declarative_node()


class OGMDateTimeTestCase(unittest.TestCase):
    class DateTimeV(DateTimeNode):
        element_type = 'datetime'
        element_plural = 'datetime'

        name = String(nullable=False, unique=True)
        at = DateTime(nullable=False)

    class DateV(DateTimeNode):
        element_type = 'dt'
        element_plural = 'dt'

        name = String(nullable=False, unique=True)
        at = Date(nullable=False)

    def setUp(self):
        g = self.g = Graph(Config.from_url('test_datetime', 'root', 'root',
                                           initial_drop=True))

        g.create_all(DateTimeNode.registry)

    def testDateTime(self):
        g = self.g

        # orientdb does not store microseconds
        # so make sure the generated datetime has none
        # Timezones / UTC offset must also be stored separately
        at = datetime.now().replace(microsecond=0)

        g.datetime.create(name='now', at=at)

        returned_dt = g.datetime.query(name='now').one()

        assert returned_dt.at == at

        # FIXME This returns microseconds, so there's nothing wrong with
        # OrientDB's storage. What's breaking for the above case?
        server_now = g.datetime.create(name='server_now', at=sysdate())
        assert server_now.at >= returned_dt.at


    def testDate(self):
        g = self.g

        at = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None).date()
        g.dt.create(name='today', at=at)
        returned_dt = g.dt.query(name='today').one()
        assert returned_dt.at == at


UnicodeNode = declarative_node()

class UnicodeV(UnicodeNode):
    element_type = 'unicode'
    element_plural = 'unicode'

    name = String(nullable=False, unique=True)
    value = String(nullable=False)
    alias = EmbeddedSet(linked_to=String(), nullable=True)


class OGMUnicodeTestCase(unittest.TestCase):
    def setUp(self):
        g = self.g = Graph(Config.from_url('test_unicode', 'root', 'root',
                                           initial_drop=True))

        g.create_all(UnicodeNode.registry)

    def testUnicode(self):
        g = self.g

        data = [
            (u'general_unicode', u'unicode value\u2017\u00c5'),
            (u'special chars: single quote', u'\''),
            (u'special chars: quote', u'"'),
            (u'special chars: new line', u'\n'),
            (u'special chars: tab', u'\t'),
            (u'multiple special chars', u'\'"\n\t'),
        ]

        for name, value in data:
            g.unicode.create(name=name, value=value)
            returned_v = g.unicode.query(name=name).one()
            assert to_unicode(returned_v.value) == value

    def testCommandEncoding(self):
        g = self.g
        name = u'unicode value\u2017'
        aliases = [u'alias\u2017', u'alias\u00c5 2']

        g.unicode.create(name=name, value=u'a', alias=aliases)

        returned_v = g.unicode.query(name=name).one()
        assert set(aliases) == set([to_unicode(a) for a in returned_v.alias])


class OGMTestCase(unittest.TestCase):
    def testConfigs(self):
        configs = [
            'localhost:2424/test_config1',
            'localhost/test_config2',
            'plocal://localhost/test_config3',
            'plocal://localhost:2424/test_config4',
            'memory://localhost/test_config5',
            'memory://localhost:2424/test_config6',
        ]

        for conf in configs:
            # the following line should not raise errors
            Graph(Config.from_url(conf, 'root', 'root', initial_drop=True))


EmbeddedNode = declarative_node()


class OGMEmbeddedTestCase(unittest.TestCase):
    class EmbeddedSetV(EmbeddedNode):
        element_type = 'emb_set'
        element_plural = 'emb_set'

        name = String(nullable=False, unique=True)
        alias = EmbeddedSet(nullable=False)

    class EmbeddedMapV(EmbeddedNode):
        element_type = 'emb_map'
        element_plural = 'emb_map'

        name = String(nullable=False, unique=True)
        children = EmbeddedMap()

    def setUp(self):
        g = self.g = Graph(Config.from_url('test_embedded', 'root', 'root',
                                           initial_drop=True))

        g.create_all(EmbeddedNode.registry)

    def testEmbeddedSetCreate(self):
        g = self.g

        # OrientDB currently has a bug that allows identical entries in EmbeddedSet:
        # https://github.com/orientechnologies/orientdb/issues/3601
        # This is not planned to be fixed until v3.0, so tolerate data
        # returned as a list, and turn it to a set before the check for convenience

        name = 'embed'
        alias = ['implant', 'lodge', 'place']

        g.emb_set.create(name=name, alias=alias)
        result = g.emb_set.query(name=name).one()

        self.assertSetEqual(set(alias), set(result.alias))

        # now try the same operation, but pass a set instead of a list
        name2 = 'embed2'
        alias2 = set(alias)

        g.emb_set.create(name=name2, alias=alias2)
        result = g.emb_set.query(name=name2).one()

        self.assertSetEqual(alias2, set(result.alias))

    def testEmbeddedMapCreate(self):
        g = self.g

        name = 'embed_map'
        children = {u'abc': u'def', 'x': 1}

        g.emb_map.create(name=name, children=children)
        result = g.emb_map.query(name=name).one()

        # if dicts A and B are subsets of each other, then they are the same dict (by value)
        self.assertDictContainsSubset(result.children, children)
        self.assertDictContainsSubset(children, result.children)

    def testEmbeddedSetContains(self):
        g = self.g

        name = 'embed'
        alias = ['implant', 'lodge', 'place']
        element_cls = g.registry['emb_set']

        g.emb_set.create(name=name, alias=alias)
        canonical_result = g.emb_set.query(name=name).one()
        self.assertIsNotNone(canonical_result)

        for alternate in alias:
            received = g.query(element_cls).filter(element_cls.alias.contains(alternate)).one()
            self.assertEqual(canonical_result, received)


class OGMEmbeddedDefaultsTestCase(unittest.TestCase):
    def setUp(self):
        g = self.g = Graph(Config.from_url('test_embedded_defaults', 'root', 'root',
                                           initial_drop=True))

    def testDefaultData(self):
        g = self.g

        g.client.command('CREATE CLASS DefaultEmbeddedNode EXTENDS V')
        g.client.command('CREATE CLASS DefaultData')
        g.client.command('CREATE PROPERTY DefaultData.normal Boolean')
        g.client.command('CREATE PROPERTY DefaultEmbeddedNode.name String')
        g.client.command('CREATE PROPERTY DefaultEmbeddedNode.info EmbeddedList DefaultData')

        try:
            g.client.command('ALTER PROPERTY DefaultData.normal DEFAULT 0')
        except PyOrientSQLParsingException as e:
            if "Unknown property attribute 'DEFAULT'" in e.errors[0]:
                # The current OrientDB version (<2.1) doesn't allow default values.
                # Simply skip this test, there's nothing we can test here.
                return
            else:
                raise

        base_node = declarative_node()
        base_relationship = declarative_relationship()
        g.include(g.build_mapping(base_node, base_relationship, auto_plural=True))

        node = g.DefaultEmbeddedNode.create(name='default_embedded')
        node.info = [{}]

        try:
            node.save()
        except PyOrientCommandException as e:
            if 'incompatible type is used.' in e.errors[0]:
                # The current OrientDB version (<2.1.5) doesn't allow embedded classes,
                # only embedded primitives (e.g. String or Int).
                # Simply skip this test, there's nothing we can test here.
                return
            else:
                raise

        # On the next load, the node should have:
        # 'info' = [{'normal': False}]
        node = g.DefaultEmbeddedNode.query().one()
        self.assertIn('normal', node.info[0])
        self.assertIs(node.info[0]['normal'], False)


if sys.version_info[0] < 3:
    def to_unicode(x):
        return str(x).decode('utf-8')
else:
    def to_unicode(x):
        return str(x)


class OGMToposortTestCase(unittest.TestCase):
    @staticmethod
    def before(classes, bf, aft):
        """Test if bf is before aft in the classes list

            Does not check if both exist
        """
        for c in classes:
            if c['name'] == bf:
                return True
            if c['name'] == aft:
                return False
        return False

    def testToposort(self):
        toposorted = Graph.toposort_classes([
            { 'name': 'A', 'superClasses': None},
            { 'name': 'B', 'superClasses': None},
            { 'name': 'C', 'superClasses': ['B']},
            { 'name': 'D', 'superClasses': ['E', 'F']},
            { 'name': 'E', 'superClasses': None},
            { 'name': 'F', 'superClasses': ['B']},
            { 'name': 'G', 'superClasses': None, 'properties': [{'linkedClass': 'H'}]},
            { 'name': 'H', 'superClasses': None}
        ])

        assert set([c['name'] for c in toposorted]) == set(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'])
        assert OGMToposortTestCase.before(toposorted, 'B', 'C')
        assert OGMToposortTestCase.before(toposorted, 'E', 'D')
        assert OGMToposortTestCase.before(toposorted, 'F', 'D')
        assert OGMToposortTestCase.before(toposorted, 'B', 'F')
        assert OGMToposortTestCase.before(toposorted, 'B', 'D')
        assert OGMToposortTestCase.before(toposorted, 'H', 'G')

    def testInfiniteLoop(self):
        # Make sure that this at least stops in case of an infinite dependency loop
        with self.assertRaises(AssertionError):
            toposorted = Graph.toposort_classes([
                { 'name': 'A', 'superClasses': ['B']},
                { 'name': 'B', 'superClasses': ['A']}
            ])


HardwareNode = declarative_node()
HardwareRelationship = declarative_relationship()


class CPU(HardwareNode):
    element_plural = 'cpu'
    name = String(nullable=False)


class X86CPU(CPU):
    element_plural = 'x86cpu'
    version = Integer(nullable=True)


class Manufacturer(HardwareNode):
    element_plural = 'manufacturer'
    name = String(nullable=False)


class Manufactures(HardwareRelationship):
    label = 'manufactures'
    out_ = Link(linked_to=Manufacturer)
    in_ = Link(linked_to=CPU)


# Added this to catch a nasty bug where toposort_classes overrode superClasses
# when reading schema from the database
class Outperforms(HardwareRelationship):
    label = 'outperforms'
    out_ = Link(linked_to=CPU)
    in_ = Link(linked_to=CPU)


class OGMTypedEdgeTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(OGMTypedEdgeTestCase, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('hardware', 'root', 'root'
                                           , initial_drop=True))

        g.create_all(HardwareNode.registry)
        g.create_all(HardwareRelationship.registry)

    def testConstraints(self):
        g = self.g
        pentium = g.cpu.create(name='Pentium')
        intel = g.manufacturer.create(name='Intel')

        # Now the constraints are enforced
        with self.assertRaises(PyOrientCommandException):
            g.manufactures.create(pentium, pentium)

        g.manufactures.create(intel, pentium)
        loaded_pentium = g.manufacturer.query().what(expand(distinct(out(Manufactures)))).all()
        assert loaded_pentium == [pentium]

    def testRegistryLoading(self):
        g = self.g

        database_registry = g.build_mapping(
            declarative_node(), declarative_relationship(), auto_plural=True)
        g.clear_registry()
        g.include(database_registry)

        manufactures_cls = g.registry['manufactures']
        assert type(manufactures_cls.in_) == Link
        assert manufactures_cls.in_.linked_to == g.registry['cpu']


class OGMTestInheritance(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(OGMTestInheritance, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('hardware', 'root', 'root'
                                           , initial_drop=True))

        g.create_all(HardwareNode.registry)
        g.create_all(HardwareRelationship.registry)

    def testInheritance(self):
        g = self.g
        pentium = g.x86cpu.create(name='Pentium', version=6)
        self.assertTrue(isinstance(pentium.name, str))
        self.assertEqual('Pentium', pentium.name)
        self.assertEqual(6, pentium.version)

        loaded_pentium = g.get_vertex(pentium._id)
        self.assertEqual(pentium, loaded_pentium)
        self.assertTrue(isinstance(loaded_pentium.name, str))

    def testStrictness(self):
        g = self.g

        # Unknown properties get silently dropped by default
        pentium = g.cpu.create(name='Pentium', version=6)
        loaded_pentium = g.get_vertex(pentium._id)
        # Version is not defined in cpu
        assert not hasattr(pentium, 'version')

        # But in strict mode they generate errors
        g = self.g = Graph(Config.from_url('hardware', 'root', 'root'
                                           , initial_drop=False), strict=True)
        g.include(g.build_mapping(
            declarative_node(), declarative_relationship(), auto_plural=True))
        with self.assertRaises(AttributeError):
            pentium = g.cpu.create(name='Pentium', version=6)

        pentium = g.x86cpu.create(name='Pentium', version=6)
        self.assertEqual('Pentium', pentium.name)
        self.assertEqual(6, pentium.version)

class OGMTestNullProperties(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(OGMTestNullProperties, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('hardware', 'root', 'root'
                                           , initial_drop=True))

        g.create_all(HardwareNode.registry)
        g.create_all(HardwareRelationship.registry)

    def testInheritance(self):
        g = self.g
        pentium = g.x86cpu.create(name='Pentium')
        loaded_pentium = g.get_vertex(pentium._id)
        self.assertIsNone(loaded_pentium.version)


ClassFieldNode = declarative_node()
ClassFieldRelationship = declarative_relationship()

class ClassFieldVertex(ClassFieldNode):
    name = String(nullable=False)

class ClassFieldVertex2(ClassFieldNode):
    name = String(nullable=False)


class ClassFieldEdge(ClassFieldRelationship):
    out_ = Link(linked_to=ClassFieldVertex)
    in_ = Link(linked_to=ClassFieldVertex)

class OGMTestClassField(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(OGMTestClassField, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('custom_field', 'root', 'root'
                                           , initial_drop=True))

        g.create_all(ClassFieldNode.registry)
        g.create_all(ClassFieldRelationship.registry)
        g.client.command('ALTER CLASS classfieldvertex CUSTOM test_field_1=test_string_one')
        g.client.command('ALTER CLASS classfieldvertex CUSTOM test_field_2="test string two"')
        g.client.command('ALTER CLASS classfieldedge CUSTOM test_field_1="test string two"')

    def testCustomFields(self):
        g = self.g

        database_registry = g.build_mapping(
            declarative_node(), declarative_relationship(), auto_plural=True)
        g.clear_registry()
        g.include(database_registry)
        if g.server_version > (2,2,0): # Ugly! TODO Isolate version at which behaviour was changed
            self.assertEqual(
                {'test_field_1': 'test_string_one', 'test_field_2': 'test string two'},
                g.registry['classfieldvertex'].class_fields)
            self.assertEqual(
                {'test_field_1': 'test string two'},
                g.registry['classfieldedge'].class_fields)
        else:
            self.assertEqual(
                {'test_field_1': 'test_string_one', 'test_field_2': '"test string two"'},
                g.registry['classfieldvertex'].class_fields)
            self.assertEqual(
                {'test_field_1': '"test string two"'},
                g.registry['classfieldedge'].class_fields)
        self.assertEqual({}, g.registry['classfieldvertex2'].class_fields)



class OGMTestAbstractField(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(OGMTestAbstractField, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('abstract_classes', 'root', 'root'
                                           , initial_drop=True))

        Node = declarative_node()
        class AbstractClass(Node.Abstract):
            element_type = 'AbstractClass'

        class ConcreteClass(Node):
            element_type = 'ConcreteClass'

        class ConcreteSubclass(AbstractClass):
            element_type = 'ConcreteSubclass'

        g.create_all(Node.registry)

    def testAbstractFlag(self):
        g = self.g

        database_registry = g.build_mapping(
            declarative_node(), declarative_relationship(), auto_plural=True)

        abstractClass = database_registry['AbstractClass']
        self.assertTrue(abstractClass.abstract)
        self.assertFalse(database_registry['ConcreteClass'].abstract)

        subclass = database_registry['ConcreteSubclass']
        self.assertFalse(subclass.abstract)
        self.assertEqual(subclass.__bases__[0], abstractClass)

class OGMTestSequences(unittest.TestCase):
    Node = declarative_node()
    class Counter(Node):
        element_plural = 'counters'

        name = String(nullable=False)
        value = Long(nullable=False, default=0)

    class Items(Node):
        element_plural = 'items'

        id = Long(nullable=False, unique=True)
        qty = Short(nullable=False)
        price = Decimal(nullable=False)

    def __init__(self, *args, **kwargs):
        super(OGMTestSequences, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('ogm_updates', 'root', 'root'
                                           , initial_drop=True))

        g.create_all(OGMTestSequences.Node.registry)
        self.mycounter = g.counters.create(name='mycounter')
        self.sequences = g.sequences


    def testSequences(self):
        g = self.g

        # A solution to auto-incrementing ids, when sequences not available (pre-OrientDB 2.2)
        Counter = OGMTestSequences.Counter

        create_first = g.batch()
        create_first['counter'] = g.counters.update().increment((Counter.value, 1)).return_(Update.Before, QV.current()).where(Counter.name=='mycounter')
        create_first[:] = create_first.items.create(id=create_first[:'counter'].value[0], qty=10, price=1000)
        create_first.commit()

        create_second = g.batch()
        create_second['counter'] = self.mycounter.update().increment((Counter.value, 1)).return_(Update.Before, QV.current())
        create_second['item'] = create_second.items.create(id=create_second[:'counter'].value[0], qty=20, price=1800)
        second_item = create_second['$item']

        self.assertEqual(second_item.id, 1)

        if g.server_version < (2, 2, 0):
            return

        # Default start value is 0, so first call to next() gives 1
        # Want it to be 2
        seq = self.sequences.create('mycounter', Sequence.Ordered, start=1)
        create_third = g.batch()
        create_third[:] = create_third.items.create(id=seq.next(), qty=30, price=2500)
        create_third.commit()

        create_fourth = g.batch()
        create_fourth['item'] = create_fourth.items.create(id=seq.next(), qty=40, price=3330)
        fourth_item = create_fourth['$item']
        self.assertEqual(fourth_item.id, 3)

        with self.assertRaises(PyOrientCommandException):
            self.sequences.create('mycounter', Sequence.Cached, 666, 2, 6)
        self.sequences.drop(seq)


        class SequencedItem(OGMTestSequences.Node):
            element_plural = 'sequenced'

            item_ids = NewSequence(Sequence.Ordered, start=-1)
            # NOTE Disabled until https://github.com/orientechnologies/orientdb/issues/7399 fixed
            #id = Long(nullable=False, unique=True, default=sequence('item_ids').next())
            id = Long(nullable=False, unique=True)
        g.create_class(SequencedItem)

        batch_create = g.batch()
        # Would be nice to use default value, but see NOTE above
        item_ids = sequence('item_ids')
        batch_create[:] = batch_create.sequenced.create(id=item_ids.next())
        batch_create[:] = batch_create.sequenced.create(id=item_ids.next())
        batch_create[:] = batch_create.sequenced.create(id=item_ids.next())
        batch_create[:] = batch_create.sequenced.create(id=item_ids.next())
        batch_create[:] = batch_create.sequenced.create(id=item_ids.next())
        batch_create['last'] = batch_create.sequenced.create(id=item_ids.next())
        last_item = batch_create['$last']
        self.assertEqual(last_item.id, 5)

class OGMTestTraversals(unittest.TestCase):
    Node = declarative_node()
    Relationship = declarative_relationship()

    class Turtle(object):
        @staticmethod
        def species():
            return 'turtle'

    class Leonardo(Node, Turtle):
        element_plural = 'leos'

    class Donatello(Node, Turtle):
        element_plural = 'dons'

    class OnTopOf(Relationship):
        label = 'on_top_of'

    class LIFO(Relationship):
        label = 'lifo'

    def __init__(self, *args, **kwargs):
        super(OGMTestTraversals, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('ogm_traversals', 'root', 'root'
                                           , initial_drop=True))

        g.create_all(OGMTestTraversals.Node.registry)
        g.create_all(OGMTestTraversals.Relationship.registry)

    def testTraversals(self):
        g = self.g

        b = g.batch()

        b['leo'] = b.leos.create()
        b['tgt'] = b[:'leo'] # Placeholder batch variable
        b['don'] = b.dons.create()
        b[:] = b[:'don'](OGMTestTraversals.OnTopOf)>b[:'leo']
        b['leo'] = b.leos.create()
        b[:] = b[:'leo'](OGMTestTraversals.OnTopOf)>b[:'don']
        b['don'] = b.dons.create()
        b[:] = b[:'don'](OGMTestTraversals.OnTopOf)>b[:'leo']
        b['leo'] = b.leos.create()
        b[:] = b[:'leo'](OGMTestTraversals.OnTopOf)>b[:'don']
        b['don'] = b.dons.create()
        b[:] = b[:'don'](OGMTestTraversals.OnTopOf)>b[:'leo']
        b['leo'] = b.leos.create()
        b[:] = b[:'leo'](OGMTestTraversals.OnTopOf)>b[:'don']
        b['don'] = b.dons.create()
        b[:] = b[:'don'](OGMTestTraversals.OnTopOf)>b[:'leo']
        b['top'] = b[:'tgt'](OGMTestTraversals.LIFO)>b[:'don']
        b['leo'] = b.leos.create()
        b[:] = b[:'leo'](OGMTestTraversals.OnTopOf)>b[:'don']
        b[:] = g.update_edge(b[:'top']).set((OGMTestTraversals.LIFO.in_, b[:'leo']))
        b['traversal'] = g.traverse(b[:'tgt'], in_(OGMTestTraversals.OnTopOf)).query().filter(QV.depth() > 0)
        traversals = b['$traversal']

        print('{}s all the way down'.format(traversals[0].species()))
        self.assertEqual(len(traversals), 8)

class OGMFetchPlansCase(unittest.TestCase):
    Node = declarative_node()
    Relationship = declarative_relationship()

    class A(Node):
        element_plural = 'ayes'

    class B(Node):
        element_plural = 'bees'

    class AB(Relationship):
        label = 'ayebee'

    def __init__(self, *args, **kwargs):
        super(OGMFetchPlansCase, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('ogm_fetchplans', 'root', 'root'
                                           , initial_drop=True))

        g.create_all(OGMFetchPlansCase.Node.registry)
        g.create_all(OGMFetchPlansCase.Relationship.registry)

    def testFetchPlans(self):
        g = self.g

        cache = {}
        b = g.batch(cache=cache)
        b['a'] = b.ayes.create()
        b['b'] = b.bees.create()
        b[:] = b.ayebee.create(b[:'a'], b[:'b'])
        b['ayes'] = b.ayes.query().fetch_plan('*:-1')
        result = b['$ayes']

        self.assertEqual(len(result), 1)
        self.assertEqual(len(cache), 2)

        prev_cache = cache.copy()
        cache.clear()
        b['ayes'] = b.ayes.query()
        b['bees'] = b.bees.query()
        result = b.collect('ayes', 'bees', fetch='*:-1')

        self.assertEqual(len(result), 2)
        # Cache will only include the edge, now
        self.assertEqual(len(cache), 1)
        from pyorient.ogm.edge import Edge
        self.assertIsInstance(cache.values()[0], Edge)

class OGMLinkResolverCase(unittest.TestCase):
    Node = declarative_node()

    class A(Node):
        element_plural = 'ayes'
        name = String(nullable=False)

    class B(Node):
        element_plural = 'bees'
    B.ayes = LinkList(linked_to=A, nullable=False)

    class C(Node):
        element_plural = 'cees'
    C.bee = Link(linked_to=B, nullable=False)

    def setUp(self):
        g = self.g = Graph(Config.from_url('ogm_linkresolver', 'root', 'root'
                                           , initial_drop=True)
                           , decorate_properties=True)

        g.create_all(OGMLinkResolverCase.Node.registry)

    def testFetchPlans(self):
        g = self.g

        cache = {}
        b = g.batch(cache=cache)
        b['a1'] = b.ayes.create(name='Foo')
        b['a2'] = b.ayes.create(name='Bar')
        b['b'] = b.bees.create(ayes=[b[:'a1'], b[:'a2']])
        b['c'] = b.cees.create(bee=b[:'b'])

        b['result'] = b.cees.query().fetch_plan('*:-1')
        c = b['$result']

        self.assertEqual(len(c), 1)
        ayes = c[0].bee.ayes
        self.assertEqual(len(ayes), 2)
        self.assertEqual(ayes[0].name, 'Foo')
        self.assertEqual(ayes[1].name, 'Bar')
        for a in ayes:
            print(a.name)

from pyorient.ogm.what import QT
class OGMTokensCase(unittest.TestCase):
    Node = declarative_node()
    Relationship = declarative_relationship()

    class Activity(Node):
        element_plural = 'activities'
        text = String(nullable=False)
        fun = Boolean(nullable=False)

    class Next(Relationship):
        label = 'next'
        probability = Float(nullable=False, readonly=True)

    def setUp(self):
        g = self.g = Graph(Config.from_url('ogm_tokens', 'root', 'root'
                                           , initial_drop=True))

        g.create_all(OGMTokensCase.Node.registry)
        g.create_all(OGMTokensCase.Relationship.registry)

        b = g.batch()
        b['a1'] = b.activities.create(text='Study', fun=False)
        b['a2'] = b.activities.create(text='Check social media', fun=True)
        b['a3'] = b.activities.create(text='Watch cat videos', fun=True)
        b['a4'] = b.activities.create(text='Go for run', fun=True)
        b['a5'] = b.activities.create(text='Read the news', fun=True)
        b['a6'] = b.activities.create(text='Have a shower', fun=True)
        b['a7'] = b.activities.create(text='Get some rest', fun=False)
        b['a8'] = b.activities.create(text='Work', fun=False)

        b[:] = b.next.create(b[:'a1'], b[:'a2'], probability=0.4)
        b[:] = b.next.create(b[:'a1'], b[:'a4'], probability=0.1)
        b[:] = b.next.create(b[:'a1'], b[:'a5'], probability=0.3)
        b[:] = b.next.create(b[:'a1'], b[:'a7'], probability=0.2)

        b[:] = b.next.create(b[:'a2'], b[:'a3'], probability=0.7)
        b[:] = b.next.create(b[:'a2'], b[:'a5'], probability=0.2)
        b[:] = b.next.create(b[:'a2'], b[:'a7'], probability=0.2)
        b[:] = b.next.create(b[:'a2'], b[:'a8'], probability=0.05)

        b[:] = b.next.create(b[:'a3'], b[:'a4'], probability=0.2)
        b[:] = b.next.create(b[:'a3'], b[:'a1'], probability=0.4)
        b[:] = b.next.create(b[:'a3'], b[:'a8'], probability=0.4)

        b[:] = b.next.create(b[:'a4'], b[:'a6'], probability=0.6)
        b[:] = b.next.create(b[:'a4'], b[:'a7'], probability=0.1)
        b[:] = b.next.create(b[:'a4'], b[:'a8'], probability=0.3)

        b[:] = b.next.create(b[:'a5'], b[:'a1'], probability=0.3)
        b[:] = b.next.create(b[:'a5'], b[:'a4'], probability=0.3)
        b[:] = b.next.create(b[:'a5'], b[:'a7'], probability=0.3)
        b[:] = b.next.create(b[:'a5'], b[:'a8'], probability=0.1)

        b[:] = b.next.create(b[:'a6'], b[:'a2'], probability=0.2)
        b[:] = b.next.create(b[:'a6'], b[:'a5'], probability=0.1)
        b[:] = b.next.create(b[:'a6'], b[:'a7'], probability=0.7)

        b[:] = b.next.create(b[:'a7'], b[:'a1'], probability=0.4)
        b[:] = b.next.create(b[:'a7'], b[:'a2'], probability=0.1)
        b[:] = b.next.create(b[:'a7'], b[:'a4'], probability=0.1)
        b[:] = b.next.create(b[:'a7'], b[:'a8'], probability=0.4)

        b[:] = b.next.create(b[:'a8'], b[:'a2'], probability=0.2)
        b[:] = b.next.create(b[:'a8'], b[:'a4'], probability=0.4)
        b[:] = b.next.create(b[:'a8'], b[:'a7'], probability=0.4)

        b.commit()

    def testTokens(self):
        g = self.g

        enjoy_query = g.activities.query().filter_by(fun=QT())
        fun = enjoy_query.format(True).all()
        self.assertEqual(len(fun), 5)
        not_fun = enjoy_query.format(False).all()
        self.assertEqual(len(not_fun), 3)

        next_query = g.next.query().what(outV().as_('o'), inV().as_('i')).filter(OGMTokensCase.Next.probability > 0.5) 
        uncached = next_query.query().what(unionall('o', 'i'))
        from copy import deepcopy
        cached = deepcopy(uncached)
        cache = {}
        cached.fetch_plan('*:1', cache)
        cached_time = timeit.timeit(lambda: cached.all(), number=30)
        uncached_time = timeit.timeit(lambda: uncached.all(), number=30)
        self.assertLess(cached_time, uncached_time)
        print("Cached query {}% faster than uncached".format(cached_time / uncached_time * 100.0))

        probable_transitions = (
            (('Have a shower', True), ('Get some rest', False))
            , (('Check social media', True), ('Watch cat videos', True))
            , (('Go for run', True), ('Have a shower', True))
        )

        # Replicate the above query, this time through token replacement
        next_query.what(QT(), QT()).filter(QT('cond'))
        token_sub = next_query.format(outV().as_('o'), inV().as_('i'), cond=OGMTokensCase.Next.probability > 0.5)

        cached = token_sub.query().what(unionall('o', 'i')).fetch_plan('*:1', cache)
        self.assertIsInstance(cached.compile(), (str, unicode))
        
        probable = cached.all()
        self.assertEqual(len(probable), 3)
        for p in probable:
            print(((p[0].text, p[0].fun), (p[1].text, p[1].fun)))
            self.assertIn(((p[0].text, p[0].fun), (p[1].text, p[1].fun)), probable_transitions)

