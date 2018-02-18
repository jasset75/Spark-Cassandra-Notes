import os, sys

import pandas as pd

os.putenv('PYTHONIOENCODING','UTF-8')

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession


# setting up Cassandra-ready spark session
# ONE consistency level is mandatory in clusters with one node
spark = (
  SparkSession.builder
    .appName('SparkCassandraApp')
    .config('spark.cassandra.connection.host', 'localhost')
    .config('spark.cassandra.connection.port', '9042')
    .config('spark.cassandra.output.consistency.level','ONE')
    .master('local[2]')
    .getOrCreate()
)

# setting error level to coherent threshold                
spark.sparkContext.setLogLevel('OFF')

sqlContext = SQLContext(spark.sparkContext)

ds_people = (
  sqlContext 
    .read
    .format('org.apache.spark.sql.cassandra')
    .options(table='mock_data', keyspace='examples')
    .load()
    .filter('drinker == "Daily"')
)

ds_cars = (
  sqlContext
    .read
    .format('org.apache.spark.sql.cassandra')
    .options(table='mock_cars', keyspace='examples')
    .load()
)

# joining datasets
ds_drinkers = (
  ds_people
    .join(ds_cars,ds_people['id'] == ds_cars['id_owner'])
    .select('id','email','car_id','car_make','car_model')
)

# create table
try:
  (
    ds_drinkers
      .createCassandraTable('examples', 'cars_owned_by_drinkers', partitionKeyColumns = ['id','car_id'])
  )
except:
  None

# write back to cassandra
(
  ds_drinkers
    .write
    .mode('append')
    .format('org.apache.spark.sql.cassandra')
    .options(table = 'cars_owned_by_drinkers', keyspace = 'examples')
    .save()
)

# loading data from the new table in Cassandra
ds_cars = (
  sqlContext
    .read
    .format('org.apache.spark.sql.cassandra')
    .options(table='cars_owned_by_drinkers', keyspace='examples')
    .load()
)

# convert to pandas
df = ds_cars.toPandas()

# json formatting
df.to_json('./output.json')