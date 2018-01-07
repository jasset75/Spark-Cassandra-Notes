# Simple Cassandra extract

Github [repository](https://github.com/jasset75/spark-cassandra-notes)

Path: [/examples/mock-example-save](https://github.com/jasset75/spark-cassandra-notes/examples/mock-example-save/)

This examples is based on [mock-example](mock-example.md).

### 1. Load data into cassandra table
*First part is equal than source example*

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

Second part is a little bit more complex.