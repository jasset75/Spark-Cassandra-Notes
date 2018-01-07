# Simple Cassandra extract

Github [repository](https://github.com/jasset75/spark-cassandra-notes)
Path: [examples/mock-example](../../examples/mock-example/)
Language: Scala v2.11

> Previous Requirements 
> * [Setting up the Environment](../Environment.md)

> Data sources
> * [Mock data of People](../PyUpload/mock_data_imp.md)

## Computing Cassandra data with Spark

This example retrieve data from Cassandra *keyspace* _**examples**_ and table name _**mockdata**_. Data are retrieved into RDD and a filter is applied. As a result only records with "Male" gender left and only "gender" and "first_name" columns are selected in a Pair RDD.

```scala
val record_names = sc.cassandraTable[(String,String)]("examples","mock_data")
                    .select("gender","first_name") //convert to RDD pair with gender and first_name columns              
                    .cache
//Male
val male_names = record_names.where("gender = 'Male'") // gender filtering 
```

When gender is filtered, append 1 to each name into Tuple2. `reduceByKey` counts `first_name` field.

```scala
val male_names_c = male_names.map{ case (k,v) => (v,1) } // associate 1 point to each male first name
val males_result = male_names_c.reduceByKey{ case (v,count) => count + count } //count 
```

So at least we have a Seq with male first names and a count `Seq[(<first_name>,n), ...]`

The same for female names.
