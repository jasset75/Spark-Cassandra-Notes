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

KEYSPACE = "examples_bis"
MOCK_DATA_TABLE = "mock_companies"
FICHERO_DATOS = "./data/mock_companies.csv"
ENCODING="utf-8"

COLUMNS = ['id','company_name','city','country','size','annual_budget']

locale.setlocale(locale.LC_ALL,'')

## Object Mapper
class MockCompanies(Model):
  __keyspace__ = KEYSPACE
  id = Integer(primary_key=True)
  company_name = Text()
  city = Text()
  country = Text()
  size = Integer()
  annual_budget = Float()

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
sync_table(MockCompanies)

## reading data from csv file
df = read_csv(os.path.abspath(FICHERO_DATOS),header=0,names=COLUMNS,quotechar='"',decimal=',',encoding=ENCODING)

df.fillna(0,inplace=True)

## saving data to database
for ind, row in tqdm(df.iterrows(), total=df.shape[0]):
  MockCompanies.create(
    id = ind,
    company_name = row['company_name'],
    city = row['city'],
    country = row['country'],
    size = row['size'],
    annual_budget = row['annual_budget']
  )
