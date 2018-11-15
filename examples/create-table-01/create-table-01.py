import os

from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy

from cassandra.cqlengine import connection
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *
from cassandra.cqlengine.management import sync_table, create_keyspace_simple

# setting up CQLENG_ALLOW_SCHEMA_MANAGEMENT to avoid warnings
os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = 'yes'

# Apache Cassandra connection
list_of_ip = ['127.0.0.1']
cluster = Cluster(list_of_ip, load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
session = cluster.connect()
connection.set_session(session)

keyspace = "keyspace_nuevo"
replication_factor = 1
durable_writes=True
connections=None

fields = dict(
  __keyspace__=keyspace,  
  car_id = UUID(primary_key=True),
  registration = Text(),
  car_make = Text(),
  car_model = Text(),
  car_model_year = Integer(),
  color = Text(),
  id_owner = Integer()
)

create_keyspace_simple(keyspace, replication_factor, durable_writes=durable_writes, connections=connections)

metaClass = type("EstoEsUnaPrueba", (Model,object), fields)

sync_table(metaClass)

