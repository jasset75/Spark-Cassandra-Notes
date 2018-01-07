# Loading cars dataset into cassandra table

> This Script load generated mock data into Cassandra "examples" keyspace and "mock-cars" table [mock_cars_imp.py](https://github.com/jasset75/spark-cassandra-notes/examples/mock-example/py-upload/mock_cars_imp.py)

## Dependencies

Some python packages are needed:

```py
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from cassandra.cqlengine.columns import *
```
>[More information about python driver for Cassandra](https://datastax.github.io/python-driver/api/index.html)

## Datase structure

```py
COLUMNS = ['car_id','registration','car_make','car_model','car_model_year','color','id_owner']
```

Defined constant COLUMNS synthetize the structure of the CSV file [mock_data.csv](https://github.com/jasset75/spark-cassandra-notes/examples/mock-example/py-upload/data/mock-cars.csv)

>This file was generated with online freemium tool [Mockaroo](http://www.mockaroo.com/) which is able to generate ramdom values into CSV format with several avalaible types.

I've choosed a set of frequently used types:

+ **car_id** UUID identifiying the record
+ **registration** car registration number
+ **car_make** brand of the car
+ **car_model** name of the model
+ **car_model_year** year of manufacture
+ **color** bodywork color of the car
+ **id_owner** id of the owner in the [collection of people](mock_data_imp.md)

[More details about data template on Mockaroo](http://www.mockaroo.com/37137260)

## Loading data from file system to memory

I use python pandas library to load CSV file into memory and to be able to work with.
```py
df = read_csv(os.path.abspath(FICHERO_DATOS),header=0,names=COLUMNS,quotechar='"',decimal=',',encoding=ENCODING)
```
>`read_csv` is very flexible method within the powerful **pandas** library which empower you to make multiple things. Load data into Dataframe structure, that is similar with Spark Dataframe; an important difference is that the last one is a distributed dataset and in pandas is locally stored in memory, but is relatively easy to convert between each other, so you could work with both at your convenience.

## Storing into Cassandra

Using this class you can do object mapping with records into Cassandra table

```py
## Object Mapper
class MockCars(Model):
  __keyspace__ = KEYSPACE
  car_id = UUID(primary_key=True)
  registration = Text()
  car_make = Text()
  car_model = Text()
  car_model_year = Integer()
  color = Text()
id_owner = Integer()
```

Simply with `sync_table(MockCars)` you can manage record persistence. Model descendant classes inherit a method to create records which will be posted into Cassandra table.

```py
## saving data to database
for ind, row in tqdm(df.iterrows(), total=df.shape[0]):
  MockData.create(
    id = ind,
    first_name = row['first_name'],
    last_name = row['last_name'],
    email = row['email'],
    gender = row['gender'],
    birth_date = row['birth_date'],
    ip_address = row['ip_address'],
    probability = row['probability'],
    smoker_bool = row['smoker_bool'],
    drinker = row['drinker'],
    language = row['language'],
    image = row['image']
)
```
