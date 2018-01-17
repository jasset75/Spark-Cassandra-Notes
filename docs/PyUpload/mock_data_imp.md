[< Back Home](../)

# Mock data of people

Loading fake information about people into cassandra table

> This Script loads generated mock data of fake people into Cassandra "examples" keyspace, in the table "mock-data" [mock_data_imp.py](https://github.com/jasset75/Spark-Cassandra-Notes/blob/master/examples/py-upload/mock_data_imp.py)

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

## Dataset structure

```py
COLUMNS = ['id','first_name','last_name','email','gender','birth_date','ip_address','probability','smoker_bool','drinker','language','image']
```

Defined constant COLUMNS synthetize the structure of the CSV file [mock_data.csv](https://github.com/jasset75/Spark-Cassandra-Notes/blob/master/examples/py-upload/data/mock-data.csv)

This file was generated with online freemium tool [Mockaroo](http://www.mockaroo.com/) which is able to generate ramdom values into CSV format with several avalaible types.

I've choosed a set of frequently used types:

+ **id** an autoincremental id
+ **first_name** first name of person
+ **last_name** last name of person
+ **email** a fake but well formed email
+ **gender** "Male" or "Female" gender
+ **birth_date** Birth date yyyy-mm-dd
+ **ip_address** an x.x.x.x IPv4 formatted field
+ **probability** classify based on Binomial distribution
+ **smoker_bool** smoker, true or false
+ **drinker**  alcoholic habits frequence: Never, Once, Seldom, Often, Daily, Weekly, Monthly, Yearly
+ **language** mother tongue: German, English, Spanish
+ **image** a fake image URL with different sizes

[More details about data template on Mockaroo](https://www.mockaroo.com/05d59200)

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
class MockData(Model):
  __keyspace__ = KEYSPACE
  id = Integer(primary_key=True)
  first_name = Text()
  last_name = Text()
  email = Text()
  gender = Text()
  birth_date = Date()
  ip_address = Text()
  probability = Float()
  smoker_bool = Boolean()
  drinker = Text()
  language = Text()
image = Text()
```

Simply with `sync_table(MockCars)` you can manage record persistence. *Model* descendant classes inherit a method to create records which will be posted into Cassandra table.

```py
for ind, row in tqdm(df.iterrows(), total=df.shape[0]):
  MockCars.create(
    car_id = row['car_id'],
    registration = row['registration'],
    car_make = row['car_make'],
    car_model = row['car_model'],
    car_model_year = row['car_model_year'],
    color = row['color'],
    id_owner = row['id_owner']
)
```
