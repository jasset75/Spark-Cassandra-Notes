# Simple Cassandra extract

Github [repository](https://github.com/jasset75/spark-cassandra-notes)
Path: [examples/mock-example](../../examples/mock-example/)

> Previous Requirements 
> * [Setting up the Environment](../Environment.md)

> Data sources
> * [Mock data of People](../PyUpload/mock_data_imp.md)

## Computing Cassandra data with Spark

This example retrieve data from a Cassandra Table called mock-data in *examples* keyspace. Data are retrieved and a filter is applied. As a result only records with "Male" gender left and only "gender" and "first_name" columns are selected in a Pair RDD.

Map male first names into tuple2 (<name>,1)
Reduced by key (name,count) adding count for equal names. So at least we have a Seq with male names grouping and dataset count 
