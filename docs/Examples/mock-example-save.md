# RDD join example

Github [repository](https://github.com/jasset75/spark-cassandra-notes)
Path: [examples/mock-example-save](../../examples/mock-example-save/)
Language: Scala v2.11

> Previous Requirements 
> * [Setting up the Environment](../Environment.md)

> Data sources
> * [Mock data of People](../PyUpload/mock_data_imp.md)

This example is similar to [mock-example](./mock-example.md)

## RDD join

First part is similar to before one, retrieving data from the same data source:

```scala
// select from Cassandra Table with Cassandra-Scala type conversion
val record_names = sc.cassandraTable("examples","mock_data")
                    .cache
// Male
val male_names = record_names
                    .select("gender","first_name") // convert to RDD pair with gender and first_name columns
                    .where("gender = 'Male'") // gender filtering 
                    .as( (g: String, n: String) => (g,n) )

val male_names_c = male_names.map{ case (k,v) => (v,1) } // associate 1 point to each male first name
                             .cache // optimize several actions over RDD
val males_result = male_names_c.reduceByKey{ case (v,count) => count + count } // count 
```
> The same for female names.

This takes five most repeated *first names* for each gender:
```scala
// taking 5 highest male repeated names                   
val males_result_high = sc.parallelize(males_result.sortBy(_._2,false).take(5))

// taking 5 highest female repeated names                   
val females_result_high = sc.parallelize(females_result.sortBy(_._2,false).take(5))
```

United into a new RDD:

```scala
val highest = males_result_high
  .union(females_result_high)
```

In the other hand It takes from source the entire table in a RDD of [Tuple2](http://www.scala-lang.org/api/2.9.1/scala/Tuple2.html) (first_name,(<entire_record>)
> be careful with performance, but in this example the max number of records is near to 1k.

```scala
val pair_record_highest = record_names
  .select( "id", "first_name", "last_name", "email", "gender",
    "birth_date", "ip_address", "probability", "smoker_bool",
    "drinker", "language", "image") 
  .as( (id: Integer, first_name: String, last_name: String,
       email: String, gender: String, birth_date: String,ip_address: String,
       probability: Float, smoker_bool: Boolean,
       drinker: String, language: String, image: String) =>
      ( first_name, 
        ( id, first_name, last_name, email, gender, birth_date, ip_address,
          probability, smoker_bool, drinker, language,image) )
    )
```

Already the two RDD's are ready to join each other, because `first_name` field is the key in both:
```scala
// RDD join
val vip_named = highest
  .join(pair_record_highest)
  .map{ case (name, ( count, row)) => row }
```


```scala
// Cassandra connector  
val cc =  CassandraConnector(sc.getConf)

// create table is not exists
cc.withSessionDo( 
  session => session.execute(
    "CREATE TABLE IF NOT EXISTS " +
    "examples.vip_named_people(" +
    "  id int PRIMARY KEY," +
    "  birth_date date," +
    "  drinker text," +
    "  email text," +
    "  first_name text," +
    "  gender text," +
    "  image text," +
    "  ip_address text," +
    "  language text," +
    "  last_name text," +
    "  probability float," +
    "  smoker_bool boolean" +
    ");"
  )
)
```

```scala
vip_named.saveToCassandra("examples","vip_named_people",SomeColumns("id","first_name","last_name","email","gender",
                          "birth_date","ip_address","probability","smoker_bool",
                          "drinker","language","image"))
```
