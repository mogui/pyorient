import sys
import unittest
import decimal
import os.path
from datetime import datetime

from pyorient.ogm import Graph, Config
from pyorient.groovy import GroovyScripts

from pyorient.ogm.declarative import declarative_node, declarative_relationship
from pyorient.ogm.property import String, DateTime, Decimal, EmbeddedMap, EmbeddedSet, Float, UUID
from pyorient.ogm.what import expand, in_, out, distinct

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

class OGMAnimalsTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(OGMAnimalsTestCase, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('animals', 'admin', 'admin'
                                           , initial_drop=True))

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

        queried_mouse = g.query(mouse).one()
        assert mouse == queried_mouse

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
        batch[::20] = batch.eats.create(batch[:'zombie'], batch[:'brains'])

        batch['unicorn'] = batch.animals.create(name='unicorn', specie='mythical')
        batch['unknown'] = batch.foods.create(name='unknown', color='rainbow')
        batch['mystery_diet'] = batch[:'unicorn'](Eats) > batch[:'unknown']

        # Commits and clears batch
        zombie = batch['$zombie']
        assert zombie.specie == 'undead'


        schema_registry = g.build_mapping(AnimalsNode, AnimalsRelationship, auto_plural=True)
        assert all(c in schema_registry for c in ['animal', 'food', 'eats'])

        assert type(schema_registry['animal'].specie) == String

        # Plurals not communicated to schema; postprocess registry before
        # include() if you have a better solution than auto_plural.
        assert schema_registry['food'].registry_plural != Food.registry_plural



MoneyNode = declarative_node()
MoneyRelationship = declarative_relationship()

class Person(MoneyNode):
    element_plural = 'people'

    full_name = String(nullable=False)
    uuid = String(nullable=False, default=UUID())

class Wallet(MoneyNode):
    element_plural = 'wallets'

    amount_precise = Decimal(name='amount', nullable=False)
    amount_imprecise = Float()

class Carries(MoneyRelationship):
    # No label set on relationship; Broker will not be attached to graph.
    pass

class OGMMoneyTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(OGMMoneyTestCase, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('money', 'admin', 'admin'
                                           , initial_drop=True))

        g.create_all(MoneyNode.registry)
        g.create_all(MoneyRelationship.registry)

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
        assert type(WalletType.amount_imprecise) == Float
        g.include(schema_registry)

        debt = decimal.Decimal(-42.0)
        WalletType.objects.create(amount=debt, amount_imprecise=0)

        assert g.query(Wallet)[2].amount == -42

class OGMClassTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(OGMClassTestCase, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('classes', 'admin', 'admin'
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

    def setUp(self):
        g = self.g = Graph(Config.from_url('test_datetime', 'admin', 'admin',
                                           initial_drop=True))

        g.create_all(DateTimeNode.registry)

    def testDateTime(self):
        g = self.g

        # orientdb does not store microseconds
        # so make sure the generated datetime has none
        at = datetime.now().replace(microsecond=0)

        g.datetime.create(name='now', at=at)

        returned_dt = g.datetime.query(name='now').one()

        assert returned_dt.at == at

    def testDate(self):
        g = self.g

        at = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        g.datetime.create(name='today', at=at.date())

        returned_dt = g.datetime.query(name='today').one()

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
        g = self.g = Graph(Config.from_url('test_unicode', 'admin', 'admin',
                                           initial_drop=True))

        g.create_all(UnicodeNode.registry)

    def testUnicode(self):
        g = self.g

        name = 'unicode test'

        # \u2017 = Double Low Line
        # \u00c5 = Latin Capital Letter A With Ring Above
        #          significant because python would like to represent this
        #          as \xc5 rather than \u00c5, which OrientDB doesn't support
        value = u'unicode value\u2017\u00c5'

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
            Graph(Config.from_url(conf, 'admin', 'admin', initial_drop=True))


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
        g = self.g = Graph(Config.from_url('test_embedded', 'admin', 'admin',
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


if sys.version_info[0] < 3:
    def to_unicode(x):
        return str(x).decode('utf-8')
else:
    def to_unicode(x):
        return str(x)
