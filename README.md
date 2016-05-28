# pyorient

**master**   
[![Build Status](https://travis-ci.org/mogui/pyorient.svg?branch=master)](https://travis-ci.org/mogui/pyorient) [![Coverage Status](https://coveralls.io/repos/mogui/pyorient/badge.svg?branch=master&service=github)](https://coveralls.io/github/mogui/pyorient?branch=master)

**develop**   
[![Build Status](https://travis-ci.org/mogui/pyorient.svg?branch=develop)](https://travis-ci.org/mogui/pyorient) [![Coverage Status](https://coveralls.io/repos/mogui/pyorient/badge.svg?branch=develop&service=github)](https://coveralls.io/github/mogui/pyorient?branch=develop)


[Orientdb](http://www.orientechnologies.com/) driver for python that uses the binary protocol.

Pyorient works with orientdb version 1.7 and later.
> **Warning** Some issues are experimented with record_create/record_upload and OrientDB < 2.0. These command are strongly discouraged with these versions

> **NOTICE** Prior to version 1.4.9 there was a potential SQL injection vulnerability that now is fixed.
(see [details](https://github.com/mogui/pyorient/pull/172) , [details](https://github.com/mogui/pyorient/pull/182) )

## Installation

	pip install pyorient

## How to contribute

- Fork the project
- work on **develop** branch
- Make your changes
- Add tests for it. This is important so I don't break it in a future version unintentionally
- Send me a pull request *(pull request to master will be rejected)*
- ???
- PROFIT

## How to run tests

- ensure you have `ant` and `nose` installed properly
- bootsrap orient by running `./ci/ci-start.sh` from project directory   
  *it will download latest orient and make some change on config and database for the tests*
- run with `nosetests`

## Usage
> Proper documentation will be available soon, for now you have to read the tests.

PyOrient is composed of two layers. At its foundation is the python wrapper around OrientDB's binary protocol. Built upon that - and OrientDB's own SQL language - is the Object-Graph Mapper (or OGM). The OGM layer is documented separately.

### Init the client
```python
client = pyorient.OrientDB("localhost", 2424)
session_id = client.connect( "admin", "admin" )
```

### Create a DB
```python
client.db_create( db_name, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY )
```

### Check if a DB exists
```python
client.db_exists( db_name, pyorient.STORAGE_TYPE_MEMORY )
```

### Open a DB
```python
client.db_open( db_name, "admin", "admin" )
```

### Close a DB and destroy the connection ( by OrientDB design )
```python
client.db_close()
```

### Get the the list of databases ( needs to be connected )
```python
client.db_list()
```

### Get the size of a database ( needs a DB opened )
```python
client.db_size()
```

### Get the number of records in a database in the OrientDB Server instance
```python
client.db_count_records()
```

### Send a command
```python
cluster_id = client.command( "create class my_class extends V" )
client.command(
    "insert into my_class ( 'accommodation', 'work', 'holiday' ) values( 'B&B', 'garage', 'mountain' )"
)
```

### Create a record
> **Warning** Some issues are experimented with record_create/record_upload and OrientDB < 2.0. These command are strongly discouraged with these versions

```python
rec = { '@my_class': { 'accommodation': 'house', 'work': 'office', 'holiday': 'sea' } }
rec_position = client.record_create( cluster_id, rec )
```
### Update a record
> **Warning** Some issues are experimented with record_create/record_upload and OrientDB < 2.0. These command are strongly discouraged with these versions

```python
rec3 = { '@my_class': { 'accommodation': 'hotel', 'work': 'home', 'holiday': 'hills' } }
update_success = client.record_update( rec_position._rid, rec_position._rid, rec3, rec_position._version )
```

### Load a record
```python
client.record_load( rec_position._rid )
```

### Load a record with cache
```python
def _my_callback(for_every_record):
    print(for_every_record)

client.record_load( rec_position._rid, "*:-1", _my_callback )
```

### Make a query
```python
result = client.query("select from my_class", 10, '*:0')
```

### Make an Async query
```python
def _my_callback(for_every_record):
    print(for_every_record)

result = client.query_async("select from my_class", 10, '*:0', _my_callback)
```

### Delete a record
```python
client.record_delete( cluster_id, rec_position._rid )
```

### Drop a DB
```python
client.db_drop( db_name )
```

### Create a new cluster
```python
new_cluster_id = client.data_cluster_add(
    'my_cluster_1234567', pyorient.CLUSTER_TYPE_PHYSICAL
)
```

### Reload DB ( refresh clusters info )
```python
client.db_reload()
```

### Get the range of record ids for a cluster
```python
client.data_cluster_data_range( new_cluster_id )
```

### Get the number of records in one or more clusters
```python
client.data_cluster_count( [ 1, 2, 3, 4, 11 ] )
```

### Drop a data cluster
```python
client.data_cluster_drop( new_cluster_id )
```

### Shut down the server. Requires "shutdown" permission to be set in orientdb-server-config.xml file
```python
client.shutdown( "root", "a_super_secret_password" )
```

### Transactions
```python
### use a cluster
cluster_id = 3

### execute real create to get some info
rec = { 'accommodation': 'mountain hut', 'work': 'not!', 'holiday': 'lake' }
rec_position = client.record_create( cluster_id, rec )

tx = client.tx_commit()
tx.begin()

### create a new record
rec1 = { 'accommodation': 'home', 'work': 'some work', 'holiday': 'surf' }
rec_position1 = client.record_create( -1, rec1 )

### prepare for an update
rec2 = { 'accommodation': 'hotel', 'work': 'office', 'holiday': 'mountain' }
update_record = client.record_update( cluster_id, rec_position._rid, rec2, rec_position._version )

tx.attach( rec_position1 )
tx.attach( rec_position1 )
tx.attach( update_record )
res = tx.commit()

assert res["#3:1"].holiday == 'mountain'
assert res["#3:2"].holiday == 'surf'
assert res["#3:3"].holiday == 'surf'
```

### Execute OrientDB SQL Batch
```python
cmd = ("begin;"
    "let a = create vertex set script = true;"
    "let b = select from v limit 1;"
    "let e = create edge from $a to $b;"
    "commit retry 100;")

    edge_result = self.client.batch(cmd)
```

### Persistent Connections ( Session Token )
Since version 27 is introduced an extension to allow use a token based session. This functionality must be enabled on the server config.

- In the first negotiation the client can ask for a token based authentication using the ```client.set_session_token``` method.
- The server will reply with a token or with an empty string meaning that it not support token based session and is using an old style session.
- For each request, the client will send the token and eventually it will get a new one if token lifetime ends.

When using the token based authentication, the connections can be shared between users of the same server.
```python
client = pyorient.OrientDB("localhost", 2424)
client.set_session_token( True )  # set true to enable the token based
authentication
client.db_open( "GratefulDeadConcerts", "admin", "admin" )

### store this token somewhere
sessionToken = client.get_session_token()

### destroy the old client, equals to another user/socket/ip ecc.
del client

### create a new client
client = pyorient.OrientDB("localhost", 2424)

### set the previous obtained token to re-attach to the old session
client.set_session_token( sessionToken )

### now the dbOpen is not needed to perform database operations
record = client.query( 'select from V where @rid = #9:1' )

### set the flag again to true if you want to renew the token
client.set_session_token( True )  # set true
client.db_open( "GratefulDeadConcerts", "admin", "admin" )
new_sessionToken = client.get_session_token()

assert sessionToken != new_sessionToken
```

### A GRAPH Example

The GRAPH representation of animals and its food


```python
import pyorient
client = pyorient.OrientDB("localhost", 2424)  # host, port

### open a connection (username and password)
client.connect("admin", "admin")

### create a database
client.db_create("animals", pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY)

### select to use that database
client.db_open("animals", "admin", "admin")

### Create the Vertex Animal
client.command("create class Animal extends V")

### Insert a new value
client.command("insert into Animal set name = 'rat', specie = 'rodent'")

### query the values
client.query("select * from Animal")
[<OrientRecord at 0x7f>..., ...]

### Create the vertex and insert the food values

client.command('create class Food extends V')
client.command("insert into Food set name = 'pea', color = 'green'")

### Create the edge for the Eat action
client.command('create class Eat extends E')

### Lets the rat likes to eat pea
eat_edges = client.command(
    "create edge Eat from ("
    "select from Animal where name = 'rat'"
    ") to ("
    "select from Food where name = 'pea'"
    ")"
)

### Who eats the peas?
pea_eaters = client.command("select expand( in( Eat )) from Food where name = 'pea'")
for animal in pea_eaters:
    print(animal.name, animal.specie)
'rat rodent'
...

### What each animal eats?
animal_foods = client.command("select expand( out( Eat )) from Animal")
for food in animal_foods:
    animal = client.query(
                "select name from ( select expand( in('Eat') ) from Food where name = 'pea' )"
            )[0]
    print(food.name, food.color, animal.name)
'pea green rat'
```

## Authors
- [mogui](https://github.com/mogui/)
- [ostico](https://github.com/ostico/)

## Copyright

Copyright (c) 2014 Niko Usai, Domenico Lupinetti. See LICENSE for details.
