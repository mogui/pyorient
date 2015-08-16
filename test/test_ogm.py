import unittest
import decimal
import os.path

from pyorient.ogm import Graph, Config
from pyorient.groovy import GroovyScripts

from pyorient.ogm.declarative import declarative_node, declarative_relationship
from pyorient.ogm.property import String, Decimal, Float

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
        queried_rat = g.query(Animal).filter(
            Animal.name.endswith('at') | (Animal.name == 'mouse')).one()

        assert rat == queried_rat

        try:
            rat2 = g.animals.create(name='rat', specie='rodent')
        except:
            pass
        else:
            assert False and 'Uniqueness not enforced correctly'

        pea = g.foods.create(name='pea', color='green')
        queried_pea = g.query(Food).filter_by(color='green', name='pea').one()

        assert queried_pea == pea

        rat_eats_pea = g.eats.create(queried_rat, queried_pea)

        eaters = g.inV(Food, Eats)
        assert rat in eaters

        for food_name, food_color in g.query(Food.name, Food.color):
            print(food_name, food_color) # 'pea green'

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
"""))

        pea_eaters = g.gremlin('get_eaters_of', 'pea')
        for animal in pea_eaters:
            print(animal.name, animal.specie) # 'rat rodent'

        rat_cuisine = g.gremlin('get_foods_eaten_by', (rat,))
        for food in rat_cuisine:
            print(food.name, food.color) # 'pea green'

MoneyNode = declarative_node()
MoneyRelationship = declarative_relationship()

class Person(MoneyNode):
    element_plural = 'people'

    full_name = String(nullable=False)

class Wallet(MoneyNode):
    element_plural = 'wallets'

    amount_precise = Decimal(name='amount', nullable=False)
    amount_imprecise = Float()

class Carries(MoneyRelationship):
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

        costanzo = g.people.create(full_name='Costanzo Veronesi')
        valerius = g.people.create(full_name='Valerius Burgstaller')
        oliver = g.people.create(full_name='Oliver Girard')

        original_inheritance = decimal.Decimal('1520841.74309871919')

        inheritance = g.wallets.create(
            amount_precise = original_inheritance
            , amount_imprecise = original_inheritance)

        assert inheritance.amount_precise == original_inheritance
        assert inheritance.amount_precise != inheritance.amount_imprecise

        pittance = decimal.Decimal('0.1')
        poor_pouch = g.wallets.create(
            amount_precise = pittance
            , amount_imprecise= pittance)

        assert poor_pouch.amount_precise == pittance
        assert poor_pouch.amount_precise != poor_pouch.amount_imprecise

        costanzo_claim = g.carries.create(costanzo, inheritance)
        valerius_claim = g.carries.create(valerius, inheritance)
        oliver_carries = g.carries.create(oliver, poor_pouch)

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

        assert len(bigwallet_query) == 1
        assert len(smallerwallet_query) == 1

        assert bigwallet_query.first() == inheritance
        assert smallerwallet_query.first() == poor_pouch

        for i, wallet in enumerate(g.query(Wallet)):
            print(decimal.Decimal(wallet.amount_imprecise) - wallet.amount_precise)
            assert i < 2

