# Simple Cassandra extract

Github [repository](https://github.com/jasset75/spark-cassandra-notes)

Path: [/examples/mock-example](https://github.com/jasset75/spark-cassandra-notes/examples/mock-example/)

> Previous Requirements [Setting up the Environment](../Environment.md)

This example has two parts:

### 1. Load data into cassandra table

We use a python script to load *fake* data about people from a CSV file.
>[more details](../PyUpload/mock_data_imp.md)

- Preparing upload

- If the cassandra virtualenv is not ok

> Virtualenv is highly recommended to follow the examples

```sh
$ virtualenv -p $PYTHON_HOME/python3.5 cassandra
$ source $VIRTUALENVS_HOME/cassandra/bin/activate
```

- Script execution
```sh
(cassandra) $ cd ~/spark-cassandra-notes/examples/py-upload
(cassandra) $ pip install -r requirements.txt
(cassandra) $ python mock_data_imp.py 
``` 

### 2. Computing Cassandra data with Spark

This example retrieve data from a Cassandra Table called mock-data in *examples* keyspace. Data are retrieved and a filter is applied. As a result only records with "Male" gender left and only "gender" and "first_name" columns are selected in a Pair RDD.

Map male first names into tuple2 (<name>,1)
Reduced by key (name,count) adding count for equal names. So at least we have a Seq with male names grouping and dataset count 

