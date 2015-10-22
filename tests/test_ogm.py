import unittest
import decimal
import os.path
from datetime import datetime

from pyorient.ogm import Graph, Config
from pyorient.groovy import GroovyScripts

from pyorient.ogm.declarative import declarative_node, declarative_relationship
from pyorient.ogm.property import String, DateTime, Decimal, Float, UUID

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

class Eats(AnimalsRelationship):
    label = 'eats'

class OGMAnimalsTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(OGMAnimalsTestCase, self).__init__(*args, **kwargs)
        self.g = None

    def setUp(self):
        g = self.g = Graph(Config.from_url('animals', 'root', 'root'
                                           , initial_drop=True))

        g.create_all(AnimalsNode.registry)
        g.create_all(AnimalsRelationship.registry)

    def testGraph(self):
        assert len(AnimalsNode.registry) == 2
        assert len(AnimalsRelationship.registry) == 1

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

        rat_eats_pea = g.eats.create(queried_rat, queried_pea)
        mouse_eats_pea = g.eats.create(mouse, pea)
        mouse_eats_cheese = Eats.objects.create(mouse, cheese)

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
        g = self.g = Graph(Config.from_url('money', 'root', 'root'
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

    def setUp(self):
        g = self.g = Graph(Config.from_url('test_datetime', 'root', 'root',
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


UnicodeNode = declarative_node()


class OGMUnicodeTestCase(unittest.TestCase):
    class UnicodeV(UnicodeNode):
        element_type = 'unicode'
        element_plural = 'unicode'

        name = String(nullable=False, unique=True)
        value = String(nullable=False)

    def setUp(self):
        g = self.g = Graph(Config.from_url('test_unicode', 'root', 'root',
                                           initial_drop=True))

        g.create_all(UnicodeNode.registry)

    def testUnicode(self):
        g = self.g

        name = 'unicode test'
        value = u'unicode value'

        g.unicode.create(name=name, value=value)

        returned_v = g.unicode.query(name=name).one()

        assert returned_v.value == value
