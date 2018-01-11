[< Back Home](../)

# Mock Data Save

Github [repository](https://github.com/jasset75/spark-cassandra-notes)
Path: [examples/mock-example-save](../../examples/mock-example-save/)
Language: Scala v2.11

> - Previous Requirements 
>   * [Setting up the Environment](../Environment.md)
> - Data sources
>   * [Mock data of People](../PyUpload/mock_data_imp.md)

This example starts at [mock-example](./mock-example.md)

This script takes the ten most repeated person's names, five from males plus five from females and store full records who match h* first names with this list into a new Cassandra table which has the same structure than original. Destination table is created if not exists.

## RDD join

- First part is similar to before one, retrieving data from the same data source:

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

- This takes five most repeated *first names* for each gender:

```scala
// taking 5 highest male repeated names                   
val males_result_high = sc.parallelize(males_result.sortBy(_._2,false).take(5))

// taking 5 highest female repeated names                   
val females_result_high = sc.parallelize(females_result.sortBy(_._2,false).take(5))
```

- Unite them into a new RDD:

```scala
val highest = males_result_high
  .union(females_result_high)
```

- In the other hand It takes from source the entire table in a RDD of [Tuple2](http://www.scala-lang.org/api/2.9.1/scala/Tuple2.html) (first_name,(<entire_record>)
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

- Already the two RDD's are ready to join each other, because `first_name` field is the key in both:
```scala
// RDD join
val vip_named = highest
  .join(pair_record_highest)
  .map{ case (name, ( count, row)) => row }
```

- This point needs a table at Cassandra cluster according to the RDD structure. Only it creates this one if not exists previously:
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

- Invoking `saveToCassandra` method will save them to Cassandra:
```scala
vip_named.saveToCassandra("examples","vip_named_people",SomeColumns("id","first_name","last_name","email","gender",
                          "birth_date","ip_address","probability","smoker_bool",
                          "drinker","language","image"))
```

- If you select the table that the method just created, the output have to be something similar to this:
```
cqlsh> SELECT id, first_name, last_name, gender, email FROM examples.vip_named_people;

 id  | first_name   | last_name   | gender | email
-----+--------------+-------------+--------+-------------------------------
 114 |       Natala |       Drury | Female |            ndrury36@opera.com
 363 | Barbara-anne |   Sigsworth | Female |         bsigswortha3@bing.com
  55 |      Margret | Elphinstone | Female | melphinstone1j@mayoclinic.com
 324 |       Natala |     Pococke | Female |     npococke90@washington.edu
 380 |        Sonya |      Dearth | Female |          sdearthak@unesco.org
 994 |        Ellyn |       Diehn | Female |         ediehnrm@amazon.co.jp
 707 |       Clarke |      Shovel |   Male |       cshoveljn@webeden.co.uk
 452 |       Brnaba |     O' Hern |   Male |      bohernck@kickstarter.com
 280 |        Ellyn |        Jost | Female |       ejost7s@themeforest.net
 204 |      Margret |       Jeram | Female |             mjeram5o@dell.com
 693 |        Sonya |      Rother | Female |       srotherj9@hostgator.com
 446 | Barbara-anne |        Benz | Female |         bbenzce@webeden.co.uk
 495 |        Mario |      Lackey |   Male |          mlackeydr@forbes.com
 396 |        Mario |      Silley |   Male |           msilleyb0@baidu.com
 198 |        Rabbi |    Greenrde |   Male |         rgreenrde5i@sogou.com
 307 |     Claudell |     Siggers |   Male |         csiggers8j@uol.com.br
 752 |     Claudell |       Starr |   Male |         cstarrkw@e-recht24.de
 258 |       Brnaba |     Ripping |   Male |    bripping76@squarespace.com
 968 |       Clarke |     Esparza |   Male |        cesparzaqw@arizona.edu
  24 |        Rabbi |      Mateos |   Male |       rmateoso@friendfeed.com

(20 rows)
```
