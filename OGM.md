# pyorient OGM

The purpose of an Object-Graph Mapper, OGM for short, is to make interactions with large, complex graph databases more understandable and maintainable.

Similar to an Object-Relational Mapper (ORM) used with relational databases, OGMs bridge a gap between the higher-level object-oriented concepts (classes) in your software and the vertexes and edges in your database.

The pyorient OGM design was heavily inspired by the marvellous SQLAlchemy ORM.

## Overview

Whether you are starting with an existing OrientDB schema or building one from scratch can make a difference to how you will use the mapper.

If you are working with an existing OrientDB schema, it may be more convenient to let the mapper automatically generate python classes.

On the other hand, and especially if you have already written a lot of python code, perhaps instead you would like the mapper to build the database schema from your python classes.

Whatever approach you take, once you have built up your dataset, naturally you will want to run queries against it. OrientDB provides many ways to get at your data; via SQL it has statements like SELECT and TRAVERSE. It also supports the Gremlin graph traversal language. The mapper currently lacks support for TRAVERSE, and its Gremlin support - though functional - could use work.

### Connecting to OrientDB

pyorient splits the process of connecting to your OrientDB server into two steps; specifying where and how to connect, and then actually connecting.

For these two steps, you will need to bring in two interfaces

```python
from pyorient.ogm import Graph, Config
```

**Graph** is central to the OGM. It wraps the lower-level pyorient interface **pyorient.OrientDB** and does the job of mapping your python classes to a database schema, and vice versa.

To specify which database, and which database schema, **Graph** accepts a configuration. The **Config** _classmethod_, from_url is a convenient way to supply one. Along with a URL, it also requires a username and credential for connecting to the database at that URL.

All of these assume a username and password 'root':
```python
configs = [
    'localhost/a'
    , 'plocal://localhost:2424/a'
    , 'test'
    , 'memory://localhost/test'
]

for conf in configs:
    Config.from_url(conf, 'root', 'root')
```

The first two and the last two configs, above, denote _the same database_. The first called _a_, and the second called _test_. A port number of 2424 will be assumed when none is given. See the OrientDB documentation for more information about _Paginated Local Storage_. 


> If you browse the OGM tests from the pyorient repository, you will notice an optional argument they use for their **Config**:
```python
g = self.g = Graph(Config.from_url('hardware', 'root', 'root'
                                   , initial_drop=True))
```
> This is done as a shorthand for clearing the previous database used for the same test.

### Building a Schema

Python classes will only be mapped to a database schema if they belong to a _registry_ supplied to your **Graph**.

There are two types of registries, one to indicate a vertex or node type, and the other an edge or relationship type. Adding your python class to a registry is a matter of subclassing:

```python
from pyorient.ogm.declarative import declarative_node, declarative_relationship

Node = declarative_node()
Relationship = declarative_relationship()

class Person(Node):
    pass

class Likes(Relationship):
    pass
```

Each call to declarative_node() and declarative_relationship() will create a new registry. 

> The mapper will preserve inheritance hierarchies of nodes and relationships.

To create the corresponding classes in the database schema, we pass registries to **Graph**:
```python
self.g.create_all(Node.registry)
self.g.create_all(Relationship.registry)
```

> If the classes already exist in your database schema, and you merely want your python classes to be bound, use the **Graph** _include_ method, which similarly accepts registries.

### Vertex and Edge Brokers

Object-Graph Mapping happens on a few different levels. So far we have seen the mapping between python classes and schema classes. The next level of mapping is between objects of those classes, and the actual vertexes and edges in the graph. Brokers work on this level.

When the mapper creates its mappings between your vertex and edge classes, and the corresponding classes in the database, it will create a **Broker** for each class.

From a coding perspective, brokers allow a shorthand for working with the various types of vertexes and edges in your graph. In terms of software architecture, they can also reduce coupling. They hide the classes that you have mapped, to focus on the interfaces they expose.

```python
class Foo(Node):
    element_plural = 'foos'
    name = String()

g.include(Node.registry)    # Creates 'foos' object, and sets 'objects' attribute.
g.foos.create(name='Bar')
Foo.objects.create(name='Baz')

find_bar = g.foos.query(name='Bar')
```

Here, _foos_ and _Foo.objects_ are the same thing, an instance of **Node**.**Broker**.

Plurals make mapping code a lot more readable. The mapper is actually fairly stubborn about this: if your node class lacks an *element_plural*, or your relationship class lacks a _label_, the Graph - here, _g_ - will not be given a broker for your class. 

Notice how the broker _create_ method hides whether you are working with a vertex or an edge. Manual use is more verbose:
```python
g.create_vertex(Foo, name='Bar')
find_bar = g.query(Foo, name='Bar')
```

### Queries and Properties

In the section above on brokers, we used, but did not elaborate much on the one *Property* of our example vertex class, _name_.

OrientDB and the pyorient mapper support a range of property types. Some of the more basic ones are:

| Numeric Types | Other |
| --------- | --------- |
| Boolean   | String    |
| Byte      | Date      |
| Integer   | DateTime  |
| Short     | Binary    |
| Long      | Embedded  |
| Float     |           |
| Double    |           |
| Decimal   |           |

> See the OrientDB documentation for more information on these and other types.

---

> Soon, this documentation will include an overview of querying.

### Automatically Generating Classes

PyOrient does not (yet?) ship with a tool for generating python code from a database schema, but it does go half way there. Using the *build_mapping* method, you can generate a dictionary of python classes (a _registry_).

This dictionary is suitable for passing to the **Graph** _include_ method.

```python
from pyorient.ogm.declarative import declarative_node, declarative_relationship

SchemaNode = declarative_node()
SchemaRelationship = declarative_relationship()

classes_from_schema = g.build_mapping(SchemaNode, SchemaRelationship, auto_plural=True)

g.include(classes_from_schema)
```

In this example, the dynamically generated vertex types will have SchemaNode at the top of their inheritance tree, and edge types SchemaRelationship. Setting *auto_plural=True* means the subsequent _include_ will assign brokers to the *Graph*, _g_.

> If you want more nicely named brokers (with actual plural nouns), you will need a custom post-process of the registry returned by *build_mapping*.

### Batching 

The mapper has basic support for transactions. Besides being useful for concurrency, batching also reduces network round-trip time.

Starting a new batch is easy:
```python
batch = g.batch()
```

This new batch will contain all the brokers the **Graph** _g_ has.
> Well, wrappers around those brokers using the same names.

So if you had the following types:
```python
class Animal(Node):
    element_type = 'animal'
    element_plural = 'animals'

    name = String(nullable=False, unique=True)
    specie = String(nullable=False)

class Food(Node):
    element_type = 'food'
    element_plural = 'foods'

    name = String(nullable=False, unique=True)
    color = String(nullable=False)

class Eats(AnimalsRelationship):
    label = 'eats'
```

... then adding vertexes to your batch would look like:
```python
batch['zombie'] = batch.animals.create(name='Liv',specie='undead')
batch['brains'] = batch.foods.create(name='brains', color='grey')
```

This creates two batch variables, _zombie_ and _brains_, that you can refer to later in the batch, say to create a relationship between them:
```python
batch[:] = batch.eats.create(batch[:'zombie'], batch[:'brains']).retry(20)
```

We use slice syntax, above, when we do not need to refer to that relationship later in the batch.

A more natural, syntactic sugar for creating edges in a batch is:
```python
batch[:] = batch[:'zombie'](Eats) > batch[:'brains']
```

Batches can optionally return a value. For example, if you want to further manipulate the newly created **Food** vertex:
```python
brains = batch['$brains']
```

Otherwise, just use the _commit_ method (or an empty slice - without assignment):
```python
batch.commit()
```

> The special attribute that we have not yet discussed, *element_type*, is redundant here. More generally, it tells the mapper what name to use for the corresponding schema class. 

### Scripting

PyOrient has basic support for the *Gremlin* graph traversal, through Groovy scripts.

```python
import pathlib
from pyorient.groovy import GroovyScripts

for p in pathlib.Path('scripts/').iterdir():
    if p.is_file() and p.suffix == '.groovy':
        g.scripts.add(GroovyScripts.from_file(str(p)))
```

All the functions found in the .groovy files under scripts/ can then be called. If one of these scripts had a *find_people* function taking one argument, their last name, you could say:

```python
wallaces = g.gremlin('find_people', 'Wallace')
```

The **Scripts**._add_, and the **Graph**._gremlin_ methods take an optional _namespace_ argument, when you need such organisation.

