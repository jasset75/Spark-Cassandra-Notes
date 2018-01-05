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
MOCK_DATA_TABLE = "mock_data"
FICHERO_DATOS = "./data/mock_data.csv"
ENCODING="utf-8"
COLUMNS = ['id','first_name','last_name','email','gender','birth_date','ip_address','probability','smoker_bool','drinker','language','image']

locale.setlocale(locale.LC_ALL,'')

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
sync_table(MockData)

## reading data from csv file
df = read_csv(os.path.abspath(FICHERO_DATOS),header=0,names=COLUMNS,quotechar='"',decimal=',',encoding=ENCODING)

df.fillna(0,inplace=True)

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
