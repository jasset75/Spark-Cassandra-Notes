# Load data about people into cassandra table

> This Script load generated mock data into Cassandra "examples" keyspace and table "mock-data"

[mock_data_imp.py](https://github.com/jasset75/spark-cassandra-notes/examples/mock-example/py-upload/mock_data_imp.py)

```py
COLUMNS = ['id','first_name','last_name','email','gender','birth_date','ip_address','probability','smoker_bool','drinker','language','image']
```

Defined constant COLUMNS synthetize the structure of the CSV file [mock_data.csv](https://github.com/jasset75/spark-cassandra-notes/examples/mock-example/py-upload/data/mock-data.csv)

This file was generated with online freemium tool [Mockaroo](http://www.mockaroo.com/) which is able to generate ramdom values into CSV format with several avalaible types.

I've choosed a set of frequently used types:

+ **id** simply a record autoincremental id
+ **first_name** first name of a record person
+ **last_name** last name of a record person
+ **email** a fake but well formed email
+ **gender** "Male" or "Female" gender
+ **birth_date** Birth date yyyy-mm-dd
+ **ip_address** an x.x.x.x IPv4 formatted field
+ **probability** classify based on Binomial distribution
+ **smoker_bool** smoker, true or false
+ **drinker**  alcoholic habits frequence: Never, Once, Seldom, Often, Daily, Weekly, Monthly, Yearly
+ **language** mother tongue: German, English, Spanish
+ **image** a fake image URL with different sizes

[More details about data template on Mockaroo](https://www.mockaroo.com/b085ea10)
