[< Back Home](../)

# Mock Data
> Simple data extraction from Apache Cassandra using RDD

Github [repository](https://github.com/jasset75/spark-cassandra-notes)
Path: [examples/mock-example](../../examples/mock-example/)
Language: Scala v2.11

> - Previous Requirements 
>   * [Setting up the Environment](../Environment.md)
>   * [Scala applications template](../scala-app-template.md
)
> - Data sources
>   * [Mock data of People](../PyUpload/mock_data_imp.md)

This example retrieve data from Cassandra *keyspace* _**examples**_ and table name _**mockdata**_. Data are retrieved into RDD and filtered. It only selects "gender" and "first_name" columns in a Pair RDD. It groups by name and count taking the five most repeated male first-names. It does the same with female names and prints each list to standard output.

```scala
val record_names = sc.cassandraTable[(String,String)]("examples","mock_data")
                    .select("gender","first_name") //convert to RDD pair with gender and first_name columns              
                    .cache
//Male
val male_names = record_names.where("gender = 'Male'") // gender filtering 
```

- When gender is filtered, append 1 to each name into Tuple2 `(<first_name>,1)`, then `reduceByKey` counts `first_name` field.

```scala
val male_names_c = male_names.map{ case (k,v) => (v,1) } // associate 1 point to each male first name
val males_result = male_names_c.reduceByKey{ case (v,count) => count + count } //count 
```

So at least we have a Seq with male first names and a count `Seq[(<first_name>,n), ...]`

The same for female names.

- Last step is to take five most repeated names of each gender recorded:
```
println("Ordered Female Names count list:")
// ordered RDD by female names                            
val females_result_az = females_result.sortByKey() // key male names are sorted in asc order
females_result_az.collect.foreach(println) // print result records through stdout
// taking 5 highest female repeated names                   
println("Five highest repeated female names:")
val females_result_high = females_result.sortBy(_._2,false).take(5)
females_result_high.foreach(println)
```

The same for male records. The output would be:
```sh
Five highest repeated male names:
(Mario,2)
(Rabbi,2)
(Clarke,2)
(Claudell,2)
(Brnaba,2)

Five highest repeated female names:
(Ellyn,2)
(Margret,2)
(Sonya,2)
(Natala,2)
(Barbara-anne,2)
```
