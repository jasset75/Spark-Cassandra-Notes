[< Back Home](../)

# Mock data of Medicines

Loading fake information about pharmaceutical corporations. This dataset records information about drug manufacturer companies.

> This Script loads generated mock data of well known pharmaceutical companies, but with fake associated data, into Cassandra "mock-companies" [mock_companies_imp.py](https://github.com/jasset75/Spark-Cassandra-Notes/blob/master/examples/py-upload/mock_companies_imp.py) table within "examples_bis" keyspace.

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
> [More information about python driver for Cassandra](https://datastax.github.io/python-driver/api/index.html)

## Dataset structure

```py
COLUMNS = COLUMNS = ['id','company_name','city','country','size','annual_budget']
```

Defined constant COLUMNS synthetize the structure of the CSV file [mock_companies.csv](https://github.com/jasset75/Spark-Cassandra-Notes/blob/master/examples/py-upload/data/mock_companies.csv)

This file was generated with online freemium tool [Mockaroo](http://www.mockaroo.com/) which is able to generate ramdom values into CSV format with several avalaible types.

I've choosed a set of frequently used types:

+ **id** autoincremental id
+ **company_name** name of pharmaceutical corporation (mockaroo generates real company names)
+ **city** fake city for headquaters
+ **country** country according to city field
+ **size** company size, measured by number of employees (fake, of course)
+ **image** totally invented annual budget, estimated by company size <- field("size") * random(18000,100000)

[More details about data template on Mockaroo](https://www.mockaroo.com/450034f0)

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
class MockCompanies(Model):
  __keyspace__ = KEYSPACE
  id = Integer(primary_key=True)
  company_name = Text()
  city = Text()
  country = Text()
  size = Integer()
annual_budget = Float()
```

Simply with `sync_table(MockCars)` you can manage record persistence. *Model* descendant classes inherit a method to create records which will be posted into Cassandra table.

```py
for ind, row in tqdm(df.iterrows(), total=df.shape[0]):
  MockCompanies.create(
    id = ind,
    company_name = row['company_name'],
    city = row['city'],
    country = row['country'],
    size = row['size'],
    annual_budget = row['annual_budget']
)
```
