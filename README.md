# pyorient

[Orientdb](http://www.orientechnologies.com/) driver for python that uses the binary protocol.

Pyorient works with orientdb version 1.7 and later.


### Installation

	pip install pyorient


## Testing

To run the tests you need `nose`

	pip install nose
	
then you can run tests with:

	nosetests 


## Usage

A full range of commands will be available soon, for now you have to read the tests.

### Init the client

	client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect( "admin", "admin" )
    
### Create a DB
        client.db_create( db_name, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY )

### Check if a DB exists
	client.db_exists( db_name, pyorient.STORAGE_TYPE_MEMORY )

### Open a DB
	client.db_open( db_name, "admin", "admin" )

### Get the size of a database ( needs a DB opened )
	client.db_size()

### Get the number of records in a database in the OrientDB Server instance
	client.db_count_records()

### Send a command
	cluster_id = client.command( "create class my_class extends V" )
	client.command( 
	  "insert into my_class ( 'accommodation', 'work', 'holiday' ) values( 'B&B', 'garage', 'mountain' )"
	)

### Create a record
	rec = { '@my_class': { 'accommodation': 'house', 'work': 'office', 'holiday': 'sea' } }
	rec_position = client.record_create( cluster_id, rec )

### Load a record
	client.record_load( rec_position.rid )

### Load a record with cache
	def _my_callback(for_every_record):
		print for_every_record

	client.record_load( rec_position.rid, "*:-1", _my_callback )

### Make a query
	result = client.query("select from my_class", 10, '*:0')

### Make an Async query
	def _my_callback(for_every_record):
		print for_every_record

	result = client.query_async("select from my_class", 10, '*:0', _my_callback)

### Delete a record
	client.record_delete( cluster_id, rec_position.rid )

### Drop a DB
	client.db_drop( db_name )

### Create a new cluster
	new_cluster_id = client.data_cluster_add(
	  'my_cluster_1234567', pyorient.CLUSTER_TYPE_PHYSICAL
	)

### Reload DB ( refresh clusters info )
	client.db_reload()

### Get the range of record ids for a cluster
	client.data_cluster_data_range( new_cluster_id )

### Get the number of records in one or more clusters
	client.data_cluster_count( [ 1, 2, 3, 4, 11 ] )

### Drop a data cluster
	client.data_cluster_drop( new_cluster_id )

### Shut down the server. Requires "shutdown" permission to be set in orientdb-server-config.xml file
	client.shutdown( "root", "a_super_secret_password" )

### Transactions

	# use a cluster
	cluster_id = 3

	# execute real create to get some info
	rec = { 'accommodation': 'mountain hut', 'work': 'not!', 'holiday': 'lake' }
	rec_position = client.record_create( cluster_id, rec )
	
	tx = client.tx_commit()
	tx.begin()
	
	# create a new record
	rec1 = { 'accommodation': 'home', 'work': 'some work', 'holiday': 'surf' }
	rec_position1 = client.record_create( -1, rec1 )
	
	# prepare for an update
	rec2 = { 'accommodation': 'hotel', 'work': 'office', 'holiday': 'mountain' }
	update_record = client.record_update( cluster_id, rec_position.rid, rec2,
	                          rec_position.version )

	tx.attach( rec_position1 )
	tx.attach( rec_position1 )
	tx.attach( update_record )
	res = tx.commit()

	assert res["#3:1"].holiday == 'mountain'
	assert res["#3:2"].holiday == 'surf'
	assert res["#3:3"].holiday == 'surf'

## Execute OrientDB SQL Batch

    cmd = ("begin;"
           "let a = create vertex set script = true;"
           "let b = select from v limit 1;"
           "let e = create edge from $a to $b;"
           "commit retry 100;")

    edge_result = self.client.batch(cmd)


## A GRAPH Example

The GRAPH representation of animals and its food 


```python
import pyorient
client = pyorient.OrientDB("localhost", 2424)  # host, port

# open a connection (username and password)
client.connect("admin", "admin")

# create a database
client.db_create("animals", pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY)

# select to use that database
client.db_open("animals", "admin", "admin")

# Create the Vertex Animal
client.command("create class Animal extends V")

# Insert a new value
client.command("insert into Animal set name = 'rat', specie = 'rodent'")

# query the values 
client.query("select * from Animal")
[<OrientRecord at 0x7f…>, …]

# Create the vertex and insert the food values

client.command('create class Food extends V')
client.command("insert into Food set name = 'pea', color = 'green'")

# Create the edge for the Eat action
client.command('create class Eat extends E')

# Lets the rat likes to eat pea
eat_edges = client.command("""
        create edge Eat
        from (select from Animal where name = 'rat')
        to (select from Food where name = 'pea')
 """)
 
# Who eats the peas?
pea_eaters = client.command("select expand( in( Eat )) from Food where name = 'pea'")
for animal in pea_eaters:
    print animal.name, animal.specie
'rat rodent'
...

# What each animal eats?
animal_foods = client.command("select expand( out( Eat )) from Animal")
for food in animal_foods:
    animal = self.client.query(
                "select name from ( select expand( in('Eat') ) from Food where name = 'pea' )"
            )[0]
    print food.name, food.color, animal.name
'pea green rat'
```



## Contributions

- Fork the project.
- Make your changes.
- Add tests for it. This is important so I don’t break it in a future version unintentionally.
- Send me a pull request.
- ???
- PROFIT

## Authors
- [mogui](https://github.com/mogui/)
- [ostico](https://github.com/ostico/)

## Copyright

Copyright © 2014 Niko Usai, Domenico Lupinetti. See LICENSE for details.
