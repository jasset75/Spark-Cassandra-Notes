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
MOCK_DATA_TABLE = "mock_drugs"
FICHERO_DATOS = "./data/mock_drugs.csv"
ENCODING="utf-8"

COLUMNS = ['id_drug','drug_name','id_company','id_patient']

locale.setlocale(locale.LC_ALL,'')

## Object Mapper
class MockDrugs(Model):
  __keyspace__ = KEYSPACE
  id_drug = Integer(primary_key=True)
  drug_name = Text()
  id_company = Integer()
  id_patient = Integer()

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
sync_table(MockDrugs)

## reading data from csv file
df = read_csv(os.path.abspath(FICHERO_DATOS),header=0,names=COLUMNS,quotechar='"',decimal=',',encoding=ENCODING)

df.fillna(0,inplace=True)

## saving data to database
for ind, row in tqdm(df.iterrows(), total=df.shape[0]):
  MockDrugs.create(
    id_drug = ind,
    drug_name = row['drug_name'],
    id_company = row['id_company'],
    id_patient = row['id_patient'],
  )
