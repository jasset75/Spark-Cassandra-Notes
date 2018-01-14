[< Back Home](../)

# Save joined dataset to new Cassandra Table and to output JSON file

Github [repository](https://github.com/jasset75/spark-cassandra-notes)
Path: [examples/dataset-join-03](https://github.com/jasset75/Spark-Cassandra-Notes/tree/master/examples/dataset-join-03)
Language: Python v3.5

> - Previous Requirements 
>   * [Setting up the Environment](../Environment.md)
> - Data sources
>   * [Mock data of People](../PyUpload/mock_data_imp.md)
>   * [Mock data of Cars owned by](../PyUpload/mock_data_imp.md)

## Description

This is a standalone python example which runs directly onto python interpreter. The core is pyspark which is a python package that is the wrapper of Apache Spark.

It is very similar to previous [dataset-join-02](dataset-join-02.md).

## Explanation

Libraries used by this example:

```py
import os, sys
import pandas as pd

os.putenv('PYTHONIOENCODING','UTF-8')

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
```

Setting up Cassandra-ready spark session

```py
spark = SparkSession.builder \
	.appName('SparkCassandraApp') \
	.config('spark.cassandra.connection.host', 'localhost') \
	.config('spark.cassandra.connection.port', '9042') \
	.config('spark.cassandra.output.consistency.level','ONE') \
	.master('local[2]') \
	.getOrCreate()
```

Loading data of people

```py
ds_people = sqlContext \
	.read \
	.format('org.apache.spark.sql.cassandra') \
	.options(table='mock_data', keyspace='examples') \
	.load() \
  .filter('drinker == "Daily"')
```

The same with cars

```py
ds_cars = sqlContext \
	.read \
	.format('org.apache.spark.sql.cassandra') \
	.options(table='mock_cars', keyspace='examples') \
	.load()
```

Join cars with owners

```py
ds_drinkers = ds_people \
  .join(ds_cars,ds_people['id'] == ds_cars['id_owner']) \
  .select('id','email','car_id','car_make','car_model')
```

Table is created if no exists, otherwise exception is raise but silenced

```py
try:
  ds_drinkers \
    .createCassandraTable('examples', 'cars_owned_by_drinkers', partitionKeyColumns = ['id','car_id'])
except:
  None
```

Saving joined dataset back to Cassandra

```py
ds_drinkers \
  .write \
  .mode('append') \
  .format('org.apache.spark.sql.cassandra') \
  .options(table = 'cars_owned_by_drinkers', keyspace = 'examples') \
  .save()
```

To test the result, it loads again new data into Spark's DataFrame

```py
ds_cars = sqlContext \
	.read \
	.format('org.apache.spark.sql.cassandra') \
	.options(table='cars_owned_by_drinkers', keyspace='examples') \
	.load()
```

Loading data into pandas in order to save to file system, although It could be post in an HTTP connection or something else

```py
df = ds_cars.toPandas()
df.to_json('./output.json')
```
