[< Back Home](../)

# Save joined dataset to new Cassandra Table

Github [repository](https://github.com/jasset75/spark-cassandra-notes)
Path: [examples/dataset-join-02](https://github.com/jasset75/Spark-Cassandra-Notes/tree/master/examples/dataset-join-02)
Language: Scala v2.11

> - Previous Requirements 
>   * [Setting up the Environment](../Environment.md)
> - Data sources
>   * [Mock data of People](../PyUpload/mock_data_imp.md)
>   * [Mock data of Cars owned by](../PyUpload/mock_data_imp.md)

## Joining two [Datasets](https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.sql.Dataset)

This example is similar to previous [dataset-join-01](dataset-join-01.md).

- Libraries used by this example:

```scala
// datastax Cassandra Connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.cql.CassandraConnector

// spark sql libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.SaveMode
```

- Setting up configuration. App runs locally with two threads. Consistency output level is setting to `"ONE"`, default `"LOCAL_QUORUM"` is not possible with a cluste of only one server. 

```scala
// setting up Cassandra-ready spark session
val spark = SparkSession
  .builder()
  .appName("SparkCassandraApp")
  .config("spark.cassandra.connection.host", "localhost")
  .config("spark.cassandra.connection.port", "9042")
  //this consistency level is mandatory in clusters with one node
  .config("spark.cassandra.output.consistency.level","ONE")
  .master("local[2]")
  .getOrCreate()
```

- DB session is needed.
```scala
// db session stablishment
val connector = CassandraConnector(spark.sparkContext.getConf)
val session = connector.openSession()
```

- The form to load data from Cassandra is a different between Dataset and RDD. It is quite easier and conforting. It is more optimal filtering at this point.
```scala
    // reading datasets to join
    val dsPeople = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "mock_data", "keyspace" -> "examples"))
      .load()
      .filter("drinker == 'Daily'")

    // reading cars owned  
    val dsCars = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "mock_cars", "keyspace" -> "examples"))
      .load()
```

- Join it is easier as well. For instance: select people who own a Blue Honda.
```scala
val dsDrinkers = dsJoin.select("id","email","car_id","car_make","car_model")
```

- Dataset API supplies alternatives to direct DDL sql queries through Cassandra Connector:
```scala
try {
  dsDrinkers
    .createCassandraTable("examples", "cars_owned_by_drinkers", partitionKeyColumns = Some(Seq("id","car_id")))
} catch {
  case e: Exception => 
}
```

- Dataset API supplies conforting write back as well. `SaveMode.Append` is used to append data.
```scala
dsDrinkers
  .write
  .mode(SaveMode.Append)
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "cars_owned_by_drinkers", "keyspace" -> "examples"))
  .save()
```

- Once data have been written, check it out!
```scala
val dsCarsDrinkers = spark
  .read
  .format("org.apache.spark.sql.cassandra")
  .options(Map( "table" -> "cars_owned_by_drinkers", "keyspace" -> "examples"))
  .load()

dsCarsDrinkers.show(200,false)
```

- And the output would be:
```txt
+---+------------------------------------+-------------+-----------------------+-------------------------------+
|id |car_id                              |car_make     |car_model              |email                          |
+---+------------------------------------+-------------+-----------------------+-------------------------------+
|532|c5577b3d-58fd-45d6-b3cc-15c4ed37f7d9|Mazda        |RX-7                   |asleichtes@tiny.cc             |
|782|9578d049-958d-44e8-b611-4533c0a71074|Volvo        |XC70                   |lfredelq@go.com                |
|266|7378adb7-a965-4795-ae7d-aebf469511e4|Oldsmobile   |Achieva                |dlegerwood7e@youtu.be          |
|900|81f330da-c322-43c3-8536-bd92bc22555c|Mazda        |Mazda6                 |cbryantp0@tinypic.com          |
|807|93497ddc-bea9-4af1-927c-8a2ab24a5699|Ford         |Excursion              |bblazemf@scribd.com            |
|590|c625e58b-8b03-4df4-ac3f-f55a824f3f59|Pontiac      |Fiero                  |tdickinge@guardian.co.uk       |
|754|b8d648cb-e4b8-4c14-853c-0900531bb142|Pontiac      |LeMans                 |jsaffrinky@ebay.co.uk          |
|387|6bf737b4-3084-4af7-ace7-315136f93eda|Mercedes-Benz|E-Class                |cdimmackar@macromedia.com      |
|904|78b44210-090a-4256-ba3a-35235bb36b0e|Buick        |Rainier                |mpattlelp4@plala.or.jp         |
|492|b6d7f202-6989-4726-bc33-2fb278380602|Pontiac      |GTO                    |trainforddo@rakuten.co.jp      |
|699|5dbbdb37-88f3-4a70-8026-32849f2c2c05|Suzuki       |Aerio                  |ejendrickejf@twitter.com       |
|564|ff80ce4d-5847-4d38-b905-b211ea011e02|Oldsmobile   |Alero                  |mbrafieldfo@google.com.br      |
|779|46395c7c-77a2-401f-80eb-96bf3185ab97|Nissan       |Sentra                 |nfuggleln@usa.gov              |
|26 |5154213c-5875-40d6-9546-17255c8e278e|Mercedes-Benz|CL-Class               |bcoxonq@unc.edu                |
|796|7b6919ce-b879-4e3d-9a44-91a8ae0f6644|Pontiac      |Grand Prix             |cstrathmanm4@joomla.org        |
|149|25c0bd13-c560-4421-b958-1122bd3771a0|Chevrolet    |S10                    |mscogings45@ehow.com           |
|671|43187cd5-2316-4b81-b427-e9e7d752600a|Hyundai      |Sonata                 |criggollin@fastcompany.com     |
|903|1fe04043-4c8c-43ca-8e69-502783a6f196|Buick        |LeSabre                |lludwikiewiczp3@nps.gov        |
|635|263775e9-c07e-46a4-a693-3d62b527abbd|Buick        |Enclave                |aglandonhn@cmu.edu             |
|211|4836950f-68c4-4673-a79a-d45e67b40f5f|Plymouth     |Neon                   |dbickerstaffe5v@wp.com         |
|233|dfef6677-8ba7-49b5-bcdd-fa1d45888fcf|Ford         |E350                   |msindle6h@photobucket.com      |
|346|d50c8068-fe28-4a2b-88bb-8b1269bee640|Chevrolet    |Camaro                 |aandryushin9m@salon.com        |
|136|395c1530-32f2-4084-ae12-217cd44e7893|Ford         |Crown Victoria         |nabad3s@homestead.com          |
|405|b4b86ee1-5181-4a2f-8a83-14834b205d37|Toyota       |Camry                  |hmoralasb9@ed.gov              |
|681|93f3735d-60be-4974-b01d-ce764b972a18|Ford         |Fusion                 |cjuneix@gmpg.org               |
|940|d754be9b-9e4f-4c90-a88e-e9d93b7b4a31|Land Rover   |Range Rover            |kbeckettq4@printfriendly.com   |
|358|f9f13728-a47e-40b4-b3d8-52655fdce923|Lotus        |Evora                  |aastbury9y@mac.com             |
|695|ee1d5037-4f0c-4278-8f74-64a2c4280be1|Mercedes-Benz|CL-Class               |gredfordjb@samsung.com         |
|572|2e386c1c-075e-48f5-8dee-e9721d4c3412|Cadillac     |XLR-V                  |pshimonyfw@harvard.edu         |
|886|a3f944df-3429-42d3-b566-63c0923697ea|Mitsubishi   |L300                   |lwoolacottom@cornell.edu       |
|561|75d00261-673f-4bbb-b0b2-44604f04fc73|Nissan       |Xterra                 |bbromwichfl@nba.com            |
|185|46a14981-3479-4882-81ff-a176c90790e6|Pontiac      |Trans Sport            |emunkley55@w3.org              |
|203|f8b86ad4-6e45-43ef-ab5b-fca11c3da48a|Mercury      |Cougar                 |dmaxwale5n@zdnet.com           |
|700|d23880d0-39d2-4302-834d-758a5f7d385b|Porsche      |928                    |jneashamjg@gov.uk              |
|686|9403d296-574d-41c9-8dd9-6943555bb873|Mercury      |Grand Marquis          |hhardyj2@ocn.ne.jp             |
|811|50281ba9-8cd4-45fe-aa58-96cdfaa9ec75|Mercury      |Mountaineer            |lsottellmj@about.com           |
|331|cc5df18b-a342-4130-9db0-bf1ae7987164|Porsche      |Boxster                |crollings97@meetup.com         |
|4  |b507aeff-f208-4e45-9da3-5c67ee8bd4a8|Ford         |Aerostar               |agaydon4@ted.com               |
|400|b2228c61-112b-4d57-ad82-d53f9490fb3b|GMC          |Yukon                  |cmalbonb4@hugedomains.com      |
|175|ddb65ebd-c781-433d-a08d-9aa5c394a9be|Eagle        |Talon                  |mmccuffie4v@geocities.jp       |
|255|1b5d41f8-7b81-412a-9398-78f9125e859c|Chevrolet    |S10                    |mfulbrook73@seesaa.net         |
|515|b7187e24-7f29-4e6f-a632-989dbee52164|Toyota       |4Runner                |cthackereb@indiatimes.com      |
|687|feb4a229-4aa7-4b7f-9421-b6633140b6ac|Chevrolet    |Venture                |dwilkensonj3@paginegialle.it   |
|76 |e6e6a3d4-3283-4adc-ad19-bb00c2d48495|Subaru       |Forester               |tcleminshaw24@surveymonkey.com |
|876|a65f9c4e-a170-4f89-a80c-251ca79c9843|Chevrolet    |Express 1500           |dseamansoc@rambler.ru          |
|605|6f05d852-b43c-4c8f-bc21-17b832a1572f|Volvo        |S60                    |friachgt@phpbb.com             |
|249|144c36fc-6bbf-45c0-b0fd-5d2b6460cb01|Buick        |Park Avenue            |atesche6x@wordpress.com        |
|86 |c73802db-c188-4c7b-a8bf-e1bc7c63cf47|Lotus        |Esprit                 |ppinkard2e@alexa.com           |
|792|7231d3f7-0613-4f1d-8d2b-35112d221540|Bentley      |Continental Flying Spur|dhowsleym0@spotify.com         |
|302|98ddfe24-25e9-49b9-b791-42a886ef8752|Mitsubishi   |Chariot                |adecourcey8e@nymag.com         |
|528|4478563f-b184-4ebb-8615-d84b7c249f56|Audi         |riolet                 |omoraleseo@japanpost.jp        |
|439|7c507426-3532-40ce-9ab3-729904068ed3|Honda        |Fit                    |speyzerc7@yellowbook.com       |
|914|7f69d4de-1ffb-4ac3-b556-b01a84bfefe9|Subaru       |Legacy                 |hwhatfordpe@nba.com            |
|712|d73b662d-14be-49d7-859f-9d20e1e376c7|Dodge        |Dynasty                |ccaldecuttjs@issuu.com         |
|868|17029efb-141d-4ced-bb82-56a8c50badea|Ford         |E250                   |jtimmiso4@clickbank.net        |
|594|385511c1-43f2-4b0f-bba0-b923ed164ad1|Mitsubishi   |Pajero                 |ptrengovegi@cdbaby.com         |
|218|20815e0c-0877-4cdf-b61f-48f95f9510ea|Mazda        |Tribute                |mgutch62@tinypic.com           |
|84 |9ad097c9-2970-4480-b15b-c2098f8e6141|Ford         |Explorer Sport Trac    |lflinders2c@tiny.cc            |
|53 |a56327ed-4a43-4910-a18e-d4545d285bbe|Mazda        |RX-7                   |sgrigore1h@phoca.cz            |
|872|96735569-2d20-4575-a36e-602ff8952a10|MINI         |Cooper                 |tglentono8@unc.edu             |
|533|accde034-98ca-4926-ad58-25723c7b1897|Volkswagen   |R32                    |dkristofet@nifty.com           |
|705|f47529fe-ca9a-49df-9d98-4705dcbe971b|Acura        |NSX                    |jpearsejl@clickbank.net        |
|800|da7424ef-a2bb-4cac-8795-e231e83d1ddb|Ford         |F350                   |ljanatkam8@newsvine.com        |
|159|8818c957-eff4-410d-9d9b-8b26b47253f9|Honda        |Passport               |mgerlts4f@smugmug.com          |
|677|e59e2f3b-0252-4401-880f-42e6997beda8|Buick        |Park Avenue            |hmcenenyit@twitpic.com         |
|499|be076613-634c-4164-9168-5c2d65793940|Audi         |riolet                 |alegricedv@aol.com             |
|969|3a56978e-7724-40b1-8f72-6796cd1961b3|Pontiac      |Firebird               |nleareqx@wikimedia.org         |
|690|f2bb0131-b1a9-4746-81be-db8bd16696a6|Plymouth     |Voyager                |mdunbabinj6@admin.ch           |
|408|6f5e9145-be17-4e3c-9d4c-506d89e0fe50|Nissan       |300ZX                  |tbarleybc@ibm.com              |
|225|646f88ba-091a-42e8-8fca-ee9a49a9c28e|Volvo        |S60                    |aosgar69@who.int               |
|931|062044a1-38cf-47ec-a9ef-5867de7c9940|BMW          |X5                     |fcorkerpv@sbwire.com           |
|489|221860c4-479a-44aa-bb43-a1a70ca05dd1|Lotus        |Elise                  |plisciandridl@sina.com.cn      |
|336|3f3a6820-da3d-4912-b269-37ba06090009|Subaru       |Legacy                 |sdobell9c@youtube.com          |
|99 |819fb204-ca5c-4488-af6a-c50b4e24b2d6|Alfa Romeo   |Spider                 |aderyebarrett2r@omniture.com   |
|867|138d12ea-ee89-453f-a646-b9cf2176ee40|Mitsubishi   |Montero                |cchildrenso3@china.com.cn      |
|449|943eea59-9ea7-4117-b898-0de5b3bad50b|Volkswagen   |Scirocco               |bgemellich@blog.com            |
|882|834f8603-b758-4c0a-8915-362c473635fa|Ford         |Ranger                 |jcolvilloi@arstechnica.com     |
|986|e4ec2a2b-c5da-4071-a7c8-18d76e851588|Jaguar       |XK Series              |msnellre@addthis.com           |
|395|2c318882-4fa4-4799-8e1e-191e3fcb80c8|Chevrolet    |Uplander               |broakesaz@list-manage.com      |
|755|6d6085da-46ea-4726-9c90-f0a6a66fc2b4|Ferrari      |612 Scaglietti         |mrolingsonkz@cafepress.com     |
|960|fbd58958-17ae-4ce7-8137-b4edb17abbdd|Mercedes-Benz|190E                   |ddudderidgeqo@illinois.edu     |
|366|eab42c64-fd79-488a-addc-e107a624b58d|Cadillac     |XLR-V                  |nshenfischa6@surveymonkey.com  |
|664|f43b0025-3b28-4cb4-855e-b52296936c78|BMW          |Z4                     |knovotneig@ocn.ne.jp           |
|787|83f00240-f54d-4c44-b5d9-062f9d1d607e|Suzuki       |SX4                    |sstallybrasslv@businesswire.com|
|106|e97c2135-b69a-4195-96e9-eddc23478bbd|Ford         |EXP                    |ydeem2y@miitbeian.gov.cn       |
|454|7dde4b48-ae0a-4278-85e5-56b1349d224c|Toyota       |Corolla                |fmandrycm@newsvine.com         |
|915|c715acaa-fa00-40bd-9b2e-27547431e8b6|Acura        |RL                     |elaintonpf@goo.gl              |
|230|bc3e072d-608d-42ab-a19c-5176f9db1045|Pontiac      |Bonneville             |ahallgough6e@slashdot.org      |
|62 |2f0bb122-f3cc-4f77-b0cc-7cc668369533|Pontiac      |Torrent                |clodovichi1q@harvard.edu       |
|271|12955ad9-5905-479a-a5d9-2c74a12a62cf|Pontiac      |Bonneville             |eohalligan7j@ox.ac.uk          |
|992|ab8b79bf-0ae4-4bdd-ba7b-800340bf20b7|Volvo        |S80                    |jmutchrk@wikia.com             |
|920|f2aa4a29-d8d3-48df-9e5c-d0c25ee1f64b|Suzuki       |Swift                  |ederlpk@exblog.jp              |
|342|6511b562-99f3-46dc-8902-6ca9eb006152|Toyota       |T100                   |rbuck9i@wikispaces.com         |
|327|a461cf67-5cc5-493b-a3f9-d2f9c7aba085|Mazda        |RX-7                   |lcopyn93@nba.com               |
|953|caae32bc-449f-4b49-9d1c-61ac2f295fdd|Buick        |Rendezvous             |iwinstanleyqh@google.co.uk     |
|697|da7954e2-c84c-4f93-b7b8-d713a7f591a1|Chrysler     |Cirrus                 |vcucuzzajd@deviantart.com      |
|500|c09eb254-385f-4a16-bc8d-bfcd73570a8e|Saab         |900                    |rgoeffdw@live.com              |
|726|3a6d26a0-fcb0-43c9-9ad2-768e0d2daa36|Acura        |Integra                |vbohlmannk6@joomla.org         |
|350|e8e9eda3-08e9-4258-bd81-7a49dd5733c7|Aston Martin |V8 Vantage             |ahradsky9q@ask.com             |
|784|ef6d658f-c203-4d1a-9a4f-738a8bb75a50|Volvo        |XC60                   |bdeavels@prnewswire.com        |
|473|b02b9f6e-4e01-4659-864c-9389f36af2fa|Mercury      |Tracer                 |mharrigand5@livejournal.com    |
|596|4946ca8b-2fda-41dd-b877-316e9453a6cf|Dodge        |Durango                |edealygk@youku.com             |
|575|2c9e6551-ecb5-481a-b3e8-0d63c76262a3|Bentley      |Continental GTC        |kbulcroftfz@microsoft.com      |
|849|c996a26e-010d-4802-b33b-d93ded897e8c|Chevrolet    |Colorado               |drichienl@quantcast.com        |
|679|ecf20a68-56d0-4531-b93e-e12b3fec0e17|Honda        |Accord                 |ehebbesiv@gov.uk               |
|802|abe7b6f8-ef1f-4178-a362-54337fa3c223|Mitsubishi   |Eclipse                |cblinderma@thetimes.co.uk      |
|501|62ef80b9-b117-45b7-9282-24bfa8ae353e|Chrysler     |Voyager                |hmenaultdx@dyndns.org          |
|247|3663bdfc-941a-449e-8a2e-ae7343ae3448|Buick        |Somerset               |sallans6v@addtoany.com         |
|107|0edb48ee-6856-481f-b48f-0b4e15ac253b|Honda        |CR-X                   |aellse2z@amazon.co.uk          |
|854|17920319-cf3b-4149-81e5-c008fd47c1cc|Volkswagen   |Touareg 2              |lpoonenq@xinhuanet.com         |
|24 |c0ebeed5-3aa4-47c3-a229-fbc8bbe28541|Lincoln      |Continental            |rmateoso@friendfeed.com        |
|861|5b6dd8d9-461c-476d-8512-53e663219495|BMW          |Z3                     |cfaynenx@xinhuanet.com         |
|642|43655c28-5876-4295-88eb-858242319b74|Maserati     |Quattroporte           |atruderhu@list-manage.com      |
|936|c19409de-11fe-48c0-8457-ef2b8a98e463|Eagle        |Talon                  |ekeenq0@nsw.gov.au             |
|420|5a0c62f7-41a6-4f62-9006-28c78d53e087|Volkswagen   |Jetta                  |vstilldalebo@infoseek.co.jp    |
|621|84f11176-b9e7-4e84-94be-c81523db4612|Mercury      |Mariner                |fstickellsh9@ocn.ne.jp         |
|367|e4c27519-5ec3-49d7-8bd6-2cc468a23c20|Plymouth     |Grand Voyager          |clefleya7@vinaora.com          |
|864|b7bdbd74-eb9f-46d0-9d11-a5840c841b6b|Saab         |9000                   |fkimbleno0@dell.com            |
|884|0262971d-92a1-4bbd-bd15-b1804957bcd1|Audi         |V8                     |mlukasikok@google.nl           |
|145|ea355147-0401-4572-b881-b139b8ff6327|Hyundai      |Elantra                |nfoat41@nba.com                |
|885|31d3023e-4460-4328-82d0-c6820430eeeb|Nissan       |200SX                  |hjergol@ow.ly                  |
|682|48f0b996-ad91-4915-893c-f3138ff5912d|Mercedes-Benz|S-Class                |wdigiorgioiy@tumblr.com        |
|146|af362e0b-bb1b-403c-97ef-71f11e4a7f79|Cadillac     |XLR-V                  |nmeece42@netscape.com          |
|987|b01ccf55-a39c-49c9-ac62-692a80b72ee6|Mitsubishi   |Pajero                 |fmclevierf@prweb.com           |
|780|5d0b3cf7-5405-4238-b61e-5e6c63e04a46|Subaru       |Tribeca                |ncolgravelo@npr.org            |
|286|570234a5-abaf-4ca9-b14a-143e44421d18|Volvo        |XC70                   |ntregenna7y@vimeo.com          |
|231|6025fff5-b649-460c-b55f-3d0d4e449655|Saturn       |Ion                    |jworviell6f@yellowbook.com     |
|812|ecb86424-d673-41ad-a9e9-2be25462587b|Subaru       |Legacy                 |gunthankmk@posterous.com       |
|989|9431564f-28a6-4953-bb43-9fb69a84636c|Lexus        |ES                     |dpilipyakrh@berkeley.edu       |
|828|309de1f7-a4a6-470f-a8d5-0e5370f5d350|Mercury      |Cougar                 |cstanionn0@myspace.com         |
|643|4a5a4c68-bd9d-440e-965c-9440444f5f76|Buick        |Regal                  |dwilkissonhv@opera.com         |
|758|7a9fcdc7-e01b-4763-8e34-366c4c0e286e|Suzuki       |Grand Vitara           |zdonohoel2@shinystat.com       |
|171|891588a4-741e-4296-84ab-c64b842024a2|Volkswagen   |Cabriolet              |gbrands4r@paypal.com           |
|569|f42caffd-6934-4c37-825e-cf931b7bb497|Dodge        |Dakota                 |bdelucaft@blogspot.com         |
|332|1ed079a9-7058-4a8c-93b6-bf2ae2d34e7d|Audi         |S6                     |dhanburybrown98@devhub.com     |
|724|a41e2d2b-404e-4fb9-aff6-1aa65cd85491|Maybach      |57S                    |xmcdonoghk4@nba.com            |
|616|877bb149-a37a-4964-99f8-9512949d3afb|Jaguar       |XK                     |bgotcherh4@youku.com           |
|955|b91c2b8d-7e77-41c8-9d22-3c2ab0f74b55|Chevrolet    |Impala                 |mwindressqj@yahoo.com          |
|276|aa6fff29-47d9-4f7a-b4ef-4359e821eb27|Dodge        |Grand Caravan          |olimbrick7o@google.de          |
|534|cabfe53f-b17c-4b42-b101-6807322ade8e|Chevrolet    |Express 1500           |kbachmaneu@flickr.com          |
|111|59d2f3a2-a7cf-4da1-bfe0-40b89323caa9|GMC          |Sierra 1500            |aslowgrove33@biblegateway.com  |
|67 |0660a94e-59fa-48b3-8ac6-1c7542bcad35|Subaru       |Legacy                 |gcake1v@msu.edu                |
|776|ea2eee7c-15ad-4284-ba94-fd50cf8ff3fd|GMC          |Sonoma                 |bdenneslk@webs.com             |
|478|2af47107-975d-420d-a300-e6dfcca4ff81|Mercedes-Benz|SL-Class               |jhenrysda@ifeng.com            |
|573|30fc4c2b-55cb-456c-a56a-862cfe2a0063|BMW          |X5                     |cdufourfx@cdbaby.com           |
+---+------------------------------------+-------------+-----------------------+-------------------------------+
```

- It is importan finalize session to free resources and application terminate.
```scala
//finish
session.close()
```
