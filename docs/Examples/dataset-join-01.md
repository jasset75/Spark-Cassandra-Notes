[< Back Home](../)

# Dataset Join

Github [repository](https://github.com/jasset75/spark-cassandra-notes)
Path: [examples/dataset-join-01](https://github.com/jasset75/Spark-Cassandra-Notes/tree/master/examples/dataset-join-01)
Language: Scala v2.11

> - Previous Requirements 
>   * [Setting up the Environment](../Environment.md)
>   * [Scala applications template](../scala-app-template.md)
> - Data sources
>   * [Mock data of People](../PyUpload/mock_data_imp.md)
>   * [Mock data of Cars owned by](../PyUpload/mock_data_imp.md)

## Joining two [Datasets](https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.sql.Dataset)

> - Since Spark 2.0 Dataset API is a high-level abstraction and an user-defined view of structured and semi-structured data. Dataset is also more space efficient than [RDD](https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#resilient-distributed-datasets-rdds).
> - Datasets are typed version of DataFrames: DataFrame is Dataset (collection) of Rows.
> - [RDD](https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#resilient-distributed-datasets-rdds) for the contrary is a low-level access interface which is better for unstructured data like streams or for expressiveness.
> - [Datasets and DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes) are built on top of [RDD]https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#resilient-distributed-datasets-rdds.
> - Dataset and DataFrame are distributed as well.

## Detailed

- Libraries used by this example

```scala
// datastax Cassandra Connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.cql.CassandraConnector

// spark sql libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
```


- Setting up configuration. App runs locally with two threads.

```scala
// setting up Cassandra-ready spark session
val spark = SparkSession
              .builder()
              .appName("SparkCassandraApp")
              .config("spark.cassandra.connection.host", "localhost")
              .config("spark.cassandra.connection.port", "9042")
              .master("local[2]")
              .getOrCreate()
```

- DB session is needed.

```scala
// db session stablishment
val connector = CassandraConnector(spark.sparkContext.getConf)
val session = connector.openSession()
```

- The form to load data from Cassandra is a different between Dataset and RDD. It is quite easier and conforting.

```scala
// reading datasets to join
val dsPeople = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "mock_data", "keyspace" -> "examples"))
  .load()

val dsCars = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "mock_cars", "keyspace" -> "examples"))
  .load()
```

- Join it is easier as well. For instance: select people who own a Blue Honda.

```scala
// joining datasets
// for test only: it could be optimized filtering before join them
val dsJoin = dsPeople
  .join(dsCars,dsPeople("id") <=> dsCars("id_owner"))

// printing records of people who own blue honda cars 
val dsBlueHonda = dsJoin.filter("color == 'Blue' and car_make == 'Honda'").select("email","id","car_model","drinker")
println("People with a Blue Honda:")
dsBlueHonda.show()
```

   * Output:
```txt
People with a Blue Honda:
+--------------------+---+---------+-------+
|               email| id|car_model|drinker|
+--------------------+---+---------+-------+
| dquenby6g@jimdo.com|232|Ridgeline| Yearly|
|cesparzaqw@arizon...|968|   Accord| Weekly|
|    gbinge16@ask.com| 42|    S2000|   Once|
+--------------------+---+---------+-------+
```

- In this join, It selects people who are drinkers:

```scala
// printing records of cars owned by people who daily drink :/
val dsDrinkers = dsJoin.filter("drinker == 'Daily'").select("email","id","car_make","car_model")
println("People and his cars who drink daily:")
dsDrinkers.show(200,false)
```

   * Output:
```txt
People and his cars who drink daily:
+-------------------------------+---+-------------+-----------------------+
|email                          |id |car_make     |car_model              |
+-------------------------------+---+-------------+-----------------------+
|sgrigore1h@phoca.cz            |53 |Mazda        |RX-7                   |
|mfulbrook73@seesaa.net         |255|Chevrolet    |S10                    |
|dseamansoc@rambler.ru          |876|Chevrolet    |Express 1500           |
|dbickerstaffe5v@wp.com         |211|Plymouth     |Neon                   |
|atruderhu@list-manage.com      |642|Maserati     |Quattroporte           |
|cstrathmanm4@joomla.org        |796|Pontiac      |Grand Prix             |
|edealygk@youku.com             |596|Dodge        |Durango                |
|bdeavels@prnewswire.com        |784|Volvo        |XC60                   |
|sstallybrasslv@businesswire.com|787|Suzuki       |SX4                    |
|tcleminshaw24@surveymonkey.com |76 |Subaru       |Forester               |
|hwhatfordpe@nba.com            |914|Subaru       |Legacy                 |
|bcoxonq@unc.edu                |26 |Mercedes-Benz|CL-Class               |
|dhanburybrown98@devhub.com     |332|Audi         |S6                     |
|hmenaultdx@dyndns.org          |501|Chrysler     |Voyager                |
|mgerlts4f@smugmug.com          |159|Honda        |Passport               |
|eohalligan7j@ox.ac.uk          |271|Pontiac      |Bonneville             |
|lsottellmj@about.com           |811|Mercury      |Mountaineer            |
|sdobell9c@youtube.com          |336|Subaru       |Legacy                 |
|ahradsky9q@ask.com             |350|Aston Martin |V8 Vantage             |
|ahallgough6e@slashdot.org      |230|Pontiac      |Bonneville             |
|hhardyj2@ocn.ne.jp             |686|Mercury      |Grand Marquis          |
|aosgar69@who.int               |225|Volvo        |S60                    |
|msindle6h@photobucket.com      |233|Ford         |E350                   |
|zdonohoel2@shinystat.com       |758|Suzuki       |Grand Vitara           |
|aandryushin9m@salon.com        |346|Chevrolet    |Camaro                 |
|clefleya7@vinaora.com          |367|Plymouth     |Grand Voyager          |
|cfaynenx@xinhuanet.com         |861|BMW          |Z3                     |
|aslowgrove33@biblegateway.com  |111|GMC          |Sierra 1500            |
|criggollin@fastcompany.com     |671|Hyundai      |Sonata                 |
|emunkley55@w3.org              |185|Pontiac      |Trans Sport            |
|jcolvilloi@arstechnica.com     |882|Ford         |Ranger                 |
|mwindressqj@yahoo.com          |955|Chevrolet    |Impala                 |
|mharrigand5@livejournal.com    |473|Mercury      |Tracer                 |
|nmeece42@netscape.com          |146|Cadillac     |XLR-V                  |
|jmutchrk@wikia.com             |992|Volvo        |S80                    |
|ejendrickejf@twitter.com       |699|Suzuki       |Aerio                  |
|dhowsleym0@spotify.com         |792|Bentley      |Continental Flying Spur|
|mgutch62@tinypic.com           |218|Mazda        |Tribute                |
|ppinkard2e@alexa.com           |86 |Lotus        |Esprit                 |
|pshimonyfw@harvard.edu         |572|Cadillac     |XLR-V                  |
|cchildrenso3@china.com.cn      |867|Mitsubishi   |Montero                |
|rgoeffdw@live.com              |500|Saab         |900                    |
|jtimmiso4@clickbank.net        |868|Ford         |E250                   |
|ederlpk@exblog.jp              |920|Suzuki       |Swift                  |
|nleareqx@wikimedia.org         |969|Pontiac      |Firebird               |
|bdenneslk@webs.com             |776|GMC          |Sonoma                 |
|dlegerwood7e@youtu.be          |266|Oldsmobile   |Achieva                |
|jsaffrinky@ebay.co.uk          |754|Pontiac      |LeMans                 |
|dpilipyakrh@berkeley.edu       |989|Lexus        |ES                     |
|ncolgravelo@npr.org            |780|Subaru       |Tribeca                |
|lwoolacottom@cornell.edu       |886|Mitsubishi   |L300                   |
|ekeenq0@nsw.gov.au             |936|Eagle        |Talon                  |
|mpattlelp4@plala.or.jp         |904|Buick        |Rainier                |
|fcorkerpv@sbwire.com           |931|BMW          |X5                     |
|lcopyn93@nba.com               |327|Mazda        |RX-7                   |
|drichienl@quantcast.com        |849|Chevrolet    |Colorado               |
|aastbury9y@mac.com             |358|Lotus        |Evora                  |
|nshenfischa6@surveymonkey.com  |366|Cadillac     |XLR-V                  |
|lpoonenq@xinhuanet.com         |854|Volkswagen   |Touareg 2              |
|plisciandridl@sina.com.cn      |489|Lotus        |Elise                  |
|tdickinge@guardian.co.uk       |590|Pontiac      |Fiero                  |
|lludwikiewiczp3@nps.gov        |903|Buick        |LeSabre                |
|aellse2z@amazon.co.uk          |107|Honda        |CR-X                   |
|mbrafieldfo@google.com.br      |564|Oldsmobile   |Alero                  |
|jworviell6f@yellowbook.com     |231|Saturn       |Ion                    |
|ntregenna7y@vimeo.com          |286|Volvo        |XC70                   |
|mmccuffie4v@geocities.jp       |175|Eagle        |Talon                  |
|cdimmackar@macromedia.com      |387|Mercedes-Benz|E-Class                |
|gredfordjb@samsung.com         |695|Mercedes-Benz|CL-Class               |
|ptrengovegi@cdbaby.com         |594|Mitsubishi   |Pajero                 |
|hmcenenyit@twitpic.com         |677|Buick        |Park Avenue            |
|cbryantp0@tinypic.com          |900|Mazda        |Mazda6                 |
|ljanatkam8@newsvine.com        |800|Ford         |F350                   |
|mlukasikok@google.nl           |884|Audi         |V8                     |
|msnellre@addthis.com           |986|Jaguar       |XK Series              |
|ddudderidgeqo@illinois.edu     |960|Mercedes-Benz|190E                   |
|agaydon4@ted.com               |4  |Ford         |Aerostar               |
|bgemellich@blog.com            |449|Volkswagen   |Scirocco               |
|dwilkensonj3@paginegialle.it   |687|Chevrolet    |Venture                |
|fmclevierf@prweb.com           |987|Mitsubishi   |Pajero                 |
|ehebbesiv@gov.uk               |679|Honda        |Accord                 |
|lflinders2c@tiny.cc            |84 |Ford         |Explorer Sport Trac    |
|nabad3s@homestead.com          |136|Ford         |Crown Victoria         |
|sallans6v@addtoany.com         |247|Buick        |Somerset               |
|knovotneig@ocn.ne.jp           |664|BMW          |Z4                     |
|gbrands4r@paypal.com           |171|Volkswagen   |Cabriolet              |
|speyzerc7@yellowbook.com       |439|Honda        |Fit                    |
|crollings97@meetup.com         |331|Porsche      |Boxster                |
|asleichtes@tiny.cc             |532|Mazda        |RX-7                   |
|cmalbonb4@hugedomains.com      |400|GMC          |Yukon                  |
|dkristofet@nifty.com           |533|Volkswagen   |R32                    |
|aglandonhn@cmu.edu             |635|Buick        |Enclave                |
|hjergol@ow.ly                  |885|Nissan       |200SX                  |
|broakesaz@list-manage.com      |395|Chevrolet    |Uplander               |
|nfuggleln@usa.gov              |779|Nissan       |Sentra                 |
|bdelucaft@blogspot.com         |569|Dodge        |Dakota                 |
|gunthankmk@posterous.com       |812|Subaru       |Legacy                 |
|atesche6x@wordpress.com        |249|Buick        |Park Avenue            |
|rmateoso@friendfeed.com        |24 |Lincoln      |Continental            |
|tbarleybc@ibm.com              |408|Nissan       |300ZX                  |
|clodovichi1q@harvard.edu       |62 |Pontiac      |Torrent                |
|vcucuzzajd@deviantart.com      |697|Chrysler     |Cirrus                 |
|omoraleseo@japanpost.jp        |528|Audi         |riolet                 |
|trainforddo@rakuten.co.jp      |492|Pontiac      |GTO                    |
|cstanionn0@myspace.com         |828|Mercury      |Cougar                 |
|fstickellsh9@ocn.ne.jp         |621|Mercury      |Mariner                |
|dmaxwale5n@zdnet.com           |203|Mercury      |Cougar                 |
|lfredelq@go.com                |782|Volvo        |XC70                   |
|ccaldecuttjs@issuu.com         |712|Dodge        |Dynasty                |
|xmcdonoghk4@nba.com            |724|Maybach      |57S                    |
|iwinstanleyqh@google.co.uk     |953|Buick        |Rendezvous             |
|nfoat41@nba.com                |145|Hyundai      |Elantra                |
|cthackereb@indiatimes.com      |515|Toyota       |4Runner                |
|kbeckettq4@printfriendly.com   |940|Land Rover   |Range Rover            |
|jhenrysda@ifeng.com            |478|Mercedes-Benz|SL-Class               |
|kbachmaneu@flickr.com          |534|Chevrolet    |Express 1500           |
|bblazemf@scribd.com            |807|Ford         |Excursion              |
|vbohlmannk6@joomla.org         |726|Acura        |Integra                |
|kbulcroftfz@microsoft.com      |575|Bentley      |Continental GTC        |
|tglentono8@unc.edu             |872|MINI         |Cooper                 |
|ydeem2y@miitbeian.gov.cn       |106|Ford         |EXP                    |
|bbromwichfl@nba.com            |561|Nissan       |Xterra                 |
|rbuck9i@wikispaces.com         |342|Toyota       |T100                   |
|fmandrycm@newsvine.com         |454|Toyota       |Corolla                |
|mdunbabinj6@admin.ch           |690|Plymouth     |Voyager                |
|alegricedv@aol.com             |499|Audi         |riolet                 |
|mscogings45@ehow.com           |149|Chevrolet    |S10                    |
|olimbrick7o@google.de          |276|Dodge        |Grand Caravan          |
|cdufourfx@cdbaby.com           |573|BMW          |X5                     |
|aderyebarrett2r@omniture.com   |99 |Alfa Romeo   |Spider                 |
|adecourcey8e@nymag.com         |302|Mitsubishi   |Chariot                |
|hmoralasb9@ed.gov              |405|Toyota       |Camry                  |
|wdigiorgioiy@tumblr.com        |682|Mercedes-Benz|S-Class                |
|jneashamjg@gov.uk              |700|Porsche      |928                    |
|elaintonpf@goo.gl              |915|Acura        |RL                     |
|gcake1v@msu.edu                |67 |Subaru       |Legacy                 |
|friachgt@phpbb.com             |605|Volvo        |S60                    |
|cblinderma@thetimes.co.uk      |802|Mitsubishi   |Eclipse                |
|bgotcherh4@youku.com           |616|Jaguar       |XK                     |
|cjuneix@gmpg.org               |681|Ford         |Fusion                 |
|fkimbleno0@dell.com            |864|Saab         |9000                   |
|vstilldalebo@infoseek.co.jp    |420|Volkswagen   |Jetta                  |
|jpearsejl@clickbank.net        |705|Acura        |NSX                    |
|mrolingsonkz@cafepress.com     |755|Ferrari      |612 Scaglietti         |
|dwilkissonhv@opera.com         |643|Buick        |Regal                  |
+-------------------------------+---+-------------+-----------------------+
```

- It is importan finalize session to free resources and application terminate.
```scala
//finish
session.close()
```
