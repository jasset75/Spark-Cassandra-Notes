[< Back Home](../)

# Dataset Join

Github [repository](https://github.com/jasset75/spark-cassandra-notes)
Path: [examples/dataset-join-01](../../examples/dataset-join-01/)
Language: Scala v2.11

> - Previous Requirements 
>   * [Setting up the Environment](../Environment.md)
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
|    gbinge16@ask.com| 42|    S2000|   Once|
|cesparzaqw@arizon...|968|   Accord| Weekly|
| dquenby6g@jimdo.com|232|Ridgeline| Yearly|
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
|mmccuffie4v@geocities.jp       |175|Eagle        |Talon                  |
|bdelucaft@blogspot.com         |569|Dodge        |Dakota                 |
|jcolvilloi@arstechnica.com     |882|Ford         |Ranger                 |
|ydeem2y@miitbeian.gov.cn       |106|Ford         |EXP                    |
|dwilkensonj3@paginegialle.it   |687|Chevrolet    |Venture                |
|ederlpk@exblog.jp              |920|Suzuki       |Swift                  |
|tglentono8@unc.edu             |872|MINI         |Cooper                 |
|clodovichi1q@harvard.edu       |62 |Pontiac      |Torrent                |
|cdufourfx@cdbaby.com           |573|BMW          |X5                     |
|mwindressqj@yahoo.com          |955|Chevrolet    |Impala                 |
|rmateoso@friendfeed.com        |24 |Lincoln      |Continental            |
|bdeavels@prnewswire.com        |784|Volvo        |XC60                   |
|jhenrysda@ifeng.com            |478|Mercedes-Benz|SL-Class               |
|aglandonhn@cmu.edu             |635|Buick        |Enclave                |
|dwilkissonhv@opera.com         |643|Buick        |Regal                  |
|bbromwichfl@nba.com            |561|Nissan       |Xterra                 |
|bblazemf@scribd.com            |807|Ford         |Excursion              |
|mgerlts4f@smugmug.com          |159|Honda        |Passport               |
|lsottellmj@about.com           |811|Mercury      |Mountaineer            |
|nabad3s@homestead.com          |136|Ford         |Crown Victoria         |
|plisciandridl@sina.com.cn      |489|Lotus        |Elise                  |
|dpilipyakrh@berkeley.edu       |989|Lexus        |ES                     |
|edealygk@youku.com             |596|Dodge        |Durango                |
|cblinderma@thetimes.co.uk      |802|Mitsubishi   |Eclipse                |
|broakesaz@list-manage.com      |395|Chevrolet    |Uplander               |
|jneashamjg@gov.uk              |700|Porsche      |928                    |
|fcorkerpv@sbwire.com           |931|BMW          |X5                     |
|mlukasikok@google.nl           |884|Audi         |V8                     |
|mbrafieldfo@google.com.br      |564|Oldsmobile   |Alero                  |
|cfaynenx@xinhuanet.com         |861|BMW          |Z3                     |
|ahradsky9q@ask.com             |350|Aston Martin |V8 Vantage             |
|sgrigore1h@phoca.cz            |53 |Mazda        |RX-7                   |
|fmclevierf@prweb.com           |987|Mitsubishi   |Pajero                 |
|drichienl@quantcast.com        |849|Chevrolet    |Colorado               |
|sallans6v@addtoany.com         |247|Buick        |Somerset               |
|cstrathmanm4@joomla.org        |796|Pontiac      |Grand Prix             |
|gredfordjb@samsung.com         |695|Mercedes-Benz|CL-Class               |
|lfredelq@go.com                |782|Volvo        |XC70                   |
|nfuggleln@usa.gov              |779|Nissan       |Sentra                 |
|mfulbrook73@seesaa.net         |255|Chevrolet    |S10                    |
|crollings97@meetup.com         |331|Porsche      |Boxster                |
|ddudderidgeqo@illinois.edu     |960|Mercedes-Benz|190E                   |
|sstallybrasslv@businesswire.com|787|Suzuki       |SX4                    |
|kbachmaneu@flickr.com          |534|Chevrolet    |Express 1500           |
|speyzerc7@yellowbook.com       |439|Honda        |Fit                    |
|dhanburybrown98@devhub.com     |332|Audi         |S6                     |
|friachgt@phpbb.com             |605|Volvo        |S60                    |
|hmenaultdx@dyndns.org          |501|Chrysler     |Voyager                |
|hwhatfordpe@nba.com            |914|Subaru       |Legacy                 |
|ncolgravelo@npr.org            |780|Subaru       |Tribeca                |
|emunkley55@w3.org              |185|Pontiac      |Trans Sport            |
|vstilldalebo@infoseek.co.jp    |420|Volkswagen   |Jetta                  |
|zdonohoel2@shinystat.com       |758|Suzuki       |Grand Vitara           |
|aosgar69@who.int               |225|Volvo        |S60                    |
|knovotneig@ocn.ne.jp           |664|BMW          |Z4                     |
|ntregenna7y@vimeo.com          |286|Volvo        |XC70                   |
|gcake1v@msu.edu                |67 |Subaru       |Legacy                 |
|cstanionn0@myspace.com         |828|Mercury      |Cougar                 |
|rgoeffdw@live.com              |500|Saab         |900                    |
|ptrengovegi@cdbaby.com         |594|Mitsubishi   |Pajero                 |
|atruderhu@list-manage.com      |642|Maserati     |Quattroporte           |
|fkimbleno0@dell.com            |864|Saab         |9000                   |
|lcopyn93@nba.com               |327|Mazda        |RX-7                   |
|nfoat41@nba.com                |145|Hyundai      |Elantra                |
|omoraleseo@japanpost.jp        |528|Audi         |riolet                 |
|hmcenenyit@twitpic.com         |677|Buick        |Park Avenue            |
|dbickerstaffe5v@wp.com         |211|Plymouth     |Neon                   |
|aslowgrove33@biblegateway.com  |111|GMC          |Sierra 1500            |
|bgemellich@blog.com            |449|Volkswagen   |Scirocco               |
|aderyebarrett2r@omniture.com   |99 |Alfa Romeo   |Spider                 |
|mrolingsonkz@cafepress.com     |755|Ferrari      |612 Scaglietti         |
|lpoonenq@xinhuanet.com         |854|Volkswagen   |Touareg 2              |
|jpearsejl@clickbank.net        |705|Acura        |NSX                    |
|jmutchrk@wikia.com             |992|Volvo        |S80                    |
|lwoolacottom@cornell.edu       |886|Mitsubishi   |L300                   |
|jtimmiso4@clickbank.net        |868|Ford         |E250                   |
|dseamansoc@rambler.ru          |876|Chevrolet    |Express 1500           |
|aandryushin9m@salon.com        |346|Chevrolet    |Camaro                 |
|hjergol@ow.ly                  |885|Nissan       |200SX                  |
|criggollin@fastcompany.com     |671|Hyundai      |Sonata                 |
|mgutch62@tinypic.com           |218|Mazda        |Tribute                |
|bdenneslk@webs.com             |776|GMC          |Sonoma                 |
|vcucuzzajd@deviantart.com      |697|Chrysler     |Cirrus                 |
|agaydon4@ted.com               |4  |Ford         |Aerostar               |
|lludwikiewiczp3@nps.gov        |903|Buick        |LeSabre                |
|ekeenq0@nsw.gov.au             |936|Eagle        |Talon                  |
|dlegerwood7e@youtu.be          |266|Oldsmobile   |Achieva                |
|pshimonyfw@harvard.edu         |572|Cadillac     |XLR-V                  |
|jsaffrinky@ebay.co.uk          |754|Pontiac      |LeMans                 |
|xmcdonoghk4@nba.com            |724|Maybach      |57S                    |
|aastbury9y@mac.com             |358|Lotus        |Evora                  |
|lflinders2c@tiny.cc            |84 |Ford         |Explorer Sport Trac    |
|dkristofet@nifty.com           |533|Volkswagen   |R32                    |
|nleareqx@wikimedia.org         |969|Pontiac      |Firebird               |
|atesche6x@wordpress.com        |249|Buick        |Park Avenue            |
|nshenfischa6@surveymonkey.com  |366|Cadillac     |XLR-V                  |
|trainforddo@rakuten.co.jp      |492|Pontiac      |GTO                    |
|nmeece42@netscape.com          |146|Cadillac     |XLR-V                  |
|mharrigand5@livejournal.com    |473|Mercury      |Tracer                 |
|sdobell9c@youtube.com          |336|Subaru       |Legacy                 |
|dhowsleym0@spotify.com         |792|Bentley      |Continental Flying Spur|
|msindle6h@photobucket.com      |233|Ford         |E350                   |
|iwinstanleyqh@google.co.uk     |953|Buick        |Rendezvous             |
|vbohlmannk6@joomla.org         |726|Acura        |Integra                |
|tbarleybc@ibm.com              |408|Nissan       |300ZX                  |
|hhardyj2@ocn.ne.jp             |686|Mercury      |Grand Marquis          |
|cbryantp0@tinypic.com          |900|Mazda        |Mazda6                 |
|adecourcey8e@nymag.com         |302|Mitsubishi   |Chariot                |
|eohalligan7j@ox.ac.uk          |271|Pontiac      |Bonneville             |
|kbeckettq4@printfriendly.com   |940|Land Rover   |Range Rover            |
|dmaxwale5n@zdnet.com           |203|Mercury      |Cougar                 |
|cthackereb@indiatimes.com      |515|Toyota       |4Runner                |
|ppinkard2e@alexa.com           |86 |Lotus        |Esprit                 |
|aellse2z@amazon.co.uk          |107|Honda        |CR-X                   |
|mscogings45@ehow.com           |149|Chevrolet    |S10                    |
|kbulcroftfz@microsoft.com      |575|Bentley      |Continental GTC        |
|tdickinge@guardian.co.uk       |590|Pontiac      |Fiero                  |
|asleichtes@tiny.cc             |532|Mazda        |RX-7                   |
|mdunbabinj6@admin.ch           |690|Plymouth     |Voyager                |
|ehebbesiv@gov.uk               |679|Honda        |Accord                 |
|wdigiorgioiy@tumblr.com        |682|Mercedes-Benz|S-Class                |
|mpattlelp4@plala.or.jp         |904|Buick        |Rainier                |
|fmandrycm@newsvine.com         |454|Toyota       |Corolla                |
|cmalbonb4@hugedomains.com      |400|GMC          |Yukon                  |
|cdimmackar@macromedia.com      |387|Mercedes-Benz|E-Class                |
|jworviell6f@yellowbook.com     |231|Saturn       |Ion                    |
|tcleminshaw24@surveymonkey.com |76 |Subaru       |Forester               |
|ccaldecuttjs@issuu.com         |712|Dodge        |Dynasty                |
|gunthankmk@posterous.com       |812|Subaru       |Legacy                 |
|hmoralasb9@ed.gov              |405|Toyota       |Camry                  |
|fstickellsh9@ocn.ne.jp         |621|Mercury      |Mariner                |
|bgotcherh4@youku.com           |616|Jaguar       |XK                     |
|ahallgough6e@slashdot.org      |230|Pontiac      |Bonneville             |
|clefleya7@vinaora.com          |367|Plymouth     |Grand Voyager          |
|rbuck9i@wikispaces.com         |342|Toyota       |T100                   |
|gbrands4r@paypal.com           |171|Volkswagen   |Cabriolet              |
|ejendrickejf@twitter.com       |699|Suzuki       |Aerio                  |
|elaintonpf@goo.gl              |915|Acura        |RL                     |
|alegricedv@aol.com             |499|Audi         |riolet                 |
|bcoxonq@unc.edu                |26 |Mercedes-Benz|CL-Class               |
|olimbrick7o@google.de          |276|Dodge        |Grand Caravan          |
|cjuneix@gmpg.org               |681|Ford         |Fusion                 |
|cchildrenso3@china.com.cn      |867|Mitsubishi   |Montero                |
|ljanatkam8@newsvine.com        |800|Ford         |F350                   |
|msnellre@addthis.com           |986|Jaguar       |XK Series              |
+-------------------------------+---+-------------+-----------------------+
```

- It is importan finalize session to free resources and application terminate.
```scala
//finish
session.close()
```
