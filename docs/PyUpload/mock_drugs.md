[< Back Home](../)

# Mock data of Medicines

Loading fake information about medicines comsumption. Based on [Mock Data](mock_data_imp.md) dataset of fake people, this dataset records information about generic drugs and patient number which links with people information. Besides, each record store `id_company` that is its simulated manufacturer. This `id_company` reference [Mock Companies](mock_companies_imp.md) datafile
. 
> This Script loads generated mock data of generic medicines: list of active substances, manufacturer company, and fake references to fake patients who would take the drug, into Cassandra table "mock-drugs" [mock_drugs_imp.py](https://github.com/jasset75/Spark-Cassandra-Notes/blob/master/examples/py-upload/mock_drugs_imp.py) within "examples_bis" keyspace, in the table 

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
COLUMNS = ['id_drug','drug_name','id_company','id_patient']
```

Defined constant COLUMNS synthetize the structure of the CSV file [mock_companies.csv](https://github.com/jasset75/Spark-Cassandra-Notes/blob/master/examples/py-upload/data/mock_drugs.csv)

This file was generated with online freemium tool [Mockaroo](http://www.mockaroo.com/) which is able to generate ramdom values into CSV format with several avalaible types.

I've choosed a set of frequently used types:

+ **id_drug** autoincremental id (unique)
+ **drug_name** name of active substances or medicine composition
+ **id_company** pharmaceutical company which manufacture the drug
+ **id_patient** patient who is being treated with this drug

[More details about data template on Mockaroo](https://www.mockaroo.com/903aee60)

> this is a denormalized dataset; many to many relationship in one key-value table

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
class MockDrugs(Model):
  __keyspace__ = KEYSPACE
  id_drug = Integer(primary_key=True)
  drug_name = Text()
  id_company = Integer()
  id_patient = Integer()
```

Simply with `sync_table(MockDrugs)` you can manage record persistence. *Model* descendant classes inherit a method to create records which will be posted into Cassandra table.

```py
for ind, row in tqdm(df.iterrows(), total=df.shape[0]):
  MockDrugs.create(
    id_drug = ind,
    drug_name = row['drug_name'],
    id_company = row['id_company'],
    id_patient = row['id_patient'],
)
```
