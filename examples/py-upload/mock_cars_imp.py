import os
import locale
import numpy as np
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from cassandra.cqlengine.query import BatchQuery
from cassandra.cqlengine.columns import *
from pandas import read_csv, concat
from tqdm import tqdm

KEYSPACE = "examples"
MOCK_DATA_TABLE = "mock_cars"
FICHERO_DATOS = "./data/mock_cars.csv"
ENCODING="utf-8"

## Column Definition
COLUMNS = ['car_id','registration','car_make','car_model','car_model_year','color','id_owner']

locale.setlocale(locale.LC_ALL,'')

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

# Apache Cassandra connection
list_of_ip = ['127.0.0.1']
cluster = Cluster(list_of_ip,load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
session = cluster.connect()
connection.set_session(session)

## creating keyspace
session.execute(
  """
    CREATE KEYSPACE IF NOT EXISTS %s
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
  """ % KEYSPACE)
session.set_keyspace(KEYSPACE)

## for testing reason if table exist then drop it
session.execute("DROP TABLE IF EXISTS %s" % MOCK_DATA_TABLE)

## create CQL table
sync_table(MockCars)

## reading data from csv file
df = read_csv(os.path.abspath(FICHERO_DATOS),header=0,names=COLUMNS,quotechar='"',decimal=',',encoding=ENCODING)

df.fillna(0,inplace=True)


## saving data to database
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
