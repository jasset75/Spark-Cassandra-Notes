# Simple Cassandra extract

Github [repository](https://github.com/jasset75/spark-cassandra-notes)

Path: [/examples/mock-example-ext](https://github.com/jasset75/spark-cassandra-notes/examples/mock-example-ext/)

This examples is based on [mock-example](mock-example.md).

First part is equal:

### 1. Load data into cassandra table

> This Script load generated mock data into Cassandra "examples" keyspace and table "mock-data"

[mock_data_imp.py](https://github.com/jasset75/spark-cassandra-notes/examples/mock-example/py-upload/mock_data_imp.py)

```py
COLUMNS = ['id','first_name','last_name','email','gender','ip_address','probability','color','smoker_bool','drinker','language','image']
```

Defined constant COLUMNS synthetize the structure of the CSV file [mock_data.csv](https://github.com/jasset75/spark-cassandra-notes/examples/mock-example/py-upload/data/mock-data.csv)

This file was generated with online freemium tool [Mockaroo](http://www.mockaroo.com/) which is able to generate ramdom values into CSV format with several avalaible types.

I've choosed a set of frequently used types:

+ **id** simply a record autoincremental id
+ **first_name** first name of a record person
+ **last_name** last name of a record person
+ **email** a fake but well formed email
+ **gender** "Male" or "Female" gender
+ **ip_address** an x.x.x.x IPv4 formatted field
+ **probability** classify based on Binomial distribution
+ **color** simple color in order to cathegorize (ramdomly) records
+ **smoker_bool** smoker, true or false
+ **drinker**  alcoholic habits frequence: Never, Once, Seldom, Often, Daily, Weekly, Monthly, Yearly
+ **language** mother tongue: German, English, Spanish
+ **image** a fake image URL with different sizes

[More details](https://www.mockaroo.com/b085ea10)

> Virtualenv is highly recommended to follow the examples
- If the cassandra virtualenv is not ok

```sh
$ virtualenv -p $PYTHON_HOME/python3.5 cassandra
$ source $VIRTUALENVS_HOME/cassandra/bin/activate
```

- Script execution
```sh
(cassandra) $ cd ~/spark-cassandra-notes/examples/mock-example/py-upload
(cassandra) $ pip install -r requirements.txt
(cassandra) $ python3 
``` 

### 2. Computing Cassandra data with Spark

Second part is a little bit more complex.