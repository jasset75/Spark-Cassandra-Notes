# Cassandra Data Management (Pure Python)

## prerequisites

```sh
pip install -r requirements.txt
```


```python
import os
import locale
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy

from cassandra.cqlengine import connection
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.columns import *
from cassandra.cqlengine.management import sync_table, create_keyspace_simple
```

```python
# pandas
import pandas as pd
```

```python
# status bar for load progress
from tqdm import tqdm
```

```python
# setting up locale
locale.setlocale(locale.LC_ALL,'')
```

```python
# setting up CQLENG_ALLOW_SCHEMA_MANAGEMENT to avoid warnings
if not os.getenv('CQLENG_ALLOW_SCHEMA_MANAGEMENT'):
    os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = 'yes'
```


```python
# Apache Cassandra connection
list_of_ip = ['127.0.0.1']
cluster = Cluster(list_of_ip, load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
session = cluster.connect()
connection.set_session(session)
```

```python
# Constants
NEW_KEYSPACE = "new_keyspace"
MOCK_DATA_TABLE = "mock_cars"
FICHERO_DATOS = "./mock_cars.csv"
ENCODING="utf-8"
REPLICATION_FACTOR = 1

# Column Definition
COLUMNS = ['car_id','registration','car_make','car_model','car_model_year','color','id_owner']
```

```python
# Object Mapper
class MockCars(Model):
    __keyspace__ = NEW_KEYSPACE
    __table_name__  = MOCK_DATA_TABLE
    car_id = UUID(primary_key=True)
    registration = Text()
    car_make = Text()
    car_model = Text()
    car_model_year = Integer()
    color = Text()
    id_owner = Integer()
```

```python
# create keyspace
create_keyspace_simple(NEW_KEYSPACE, REPLICATION_FACTOR, durable_writes=True, connections=None)
```

```python
# create table if not exist
sync_table(MockCars)
```

```python
# reading data from csv file into pandas panel
df = pd.read_csv(os.path.abspath(FICHERO_DATOS),header=0,names=COLUMNS,quotechar='"',decimal=',',encoding=ENCODING)

# NaNs is not desired
df.fillna(0,inplace=True)
```



```python
# saving data to database
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
```

```python
# read from cassandra into pandas dataframe
query = 'SELECT * FROM {}.{}'.format(NEW_KEYSPACE, MOCK_DATA_TABLE)
df = pd.DataFrame(list(session.execute(query)))

# tabular terminal output
print(df)
```
