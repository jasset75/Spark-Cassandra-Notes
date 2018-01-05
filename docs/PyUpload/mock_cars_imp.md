# Load data about cars into cassandra table

> This Script load generated mock data into Cassandra "examples" keyspace and table "mock-cars"

[mock_cars_imp.py](https://github.com/jasset75/spark-cassandra-notes/examples/mock-example/py-upload/mock_cars_imp.py)

```py
COLUMNS = ['car_id','registration','car_make','car_model','car_model_year','color','id_owner']
```

Defined constant COLUMNS synthetize the structure of the CSV file [mock_data.csv](https://github.com/jasset75/spark-cassandra-notes/examples/mock-example/py-upload/data/mock-cars.csv)

This file was generated with online freemium tool [Mockaroo](http://www.mockaroo.com/) which is able to generate ramdom values into CSV format with several avalaible types.

I've choosed a set of frequently used types:

+ **car_id** UUID identifiying the record
+ **registration** car registration number
+ **car_make** brand of the car
+ **car_model** name of the model
+ **car_model_year** year of manufacture
+ **color** bodywork color of the car
+ **id_owner** id of the owner in the [collection of people](mock_data_imp.md)

[More details about data template on Mockaroo](http://www.mockaroo.com/37137260)
