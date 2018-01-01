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
FICHERO_DATOS = "mock_data.csv"
ENCODING="utf-8"
COLUMNS = ['id','first_name','last_name','email','gender','ip_address','probability','color','smoker_bool','drinker','language','image']
DTYPES = [str,str,str,str,object,float]

locale.setlocale(locale.LC_ALL,'')

## Object Mapper
class MockData(Model):
  __keyspace__ = KEYSPACE
  id = Integer(primary_key=True)
  first_name = Text()
  last_name = Text()
  email = Text()
  gender = Text()
  ip_address = Text()
  probability = Float()
  color = Text()
  smoker_bool = Boolean()
  drinker = Text()
  language = Text()
  image = Text()


### Conectando no Apache Cassandra
list_of_ip = ['127.0.0.1']
cluster = Cluster(list_of_ip,load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
session = cluster.connect()
connection.set_session(session)

## creating keyspace
session.execute("DROP KEYSPACE " + KEYSPACE)
session.execute(
  """
    CREATE KEYSPACE %s
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }
  """ % KEYSPACE)
session.set_keyspace(KEYSPACE)


## create a CQL table
sync_table(MockData)

## lendo CSV from DATASUS RD
df_go = read_csv(FICHERO_DATOS,header=0,names=COLUMNS,quotechar='"',decimal=',',encoding=ENCODING,dtype={"dato": float})

stress_factor = 1
df_go = concat(stress_factor * [df_go])
df_go.fillna(0,inplace=True)

def convert(x):
  try:
    return int(float(x))
  except:
    print('error: ',x)
    return None

## send to database

for ind, row in tqdm(df_go.iterrows(), total=df_go.shape[0]):
  MockData.create(
    id = ind,
    first_name = row['first_name'],
    last_name = row['last_name'],
    email = row['email'],
    gender = row['gender'],
    ip_address = row['ip_address'],
    probability = row['probability'],
    color = row['color'],
    smoker_bool = row['smoker_bool'],
    drinker = row['drinker'],
    language = row['language'],
    image = row['image']
  )

