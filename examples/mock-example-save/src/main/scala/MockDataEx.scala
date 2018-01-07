package uk.me.jasset.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

/** Computes an example */
object MockDataEx {
  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "localhost")
      //this consistency level is mandatory in clusters with one node
      .set("spark.cassandra.output.consistency.level","ONE")

    // spark context creation with conf
    val sc = new SparkContext(conf)
    // reducing verbosity
    sc.setLogLevel("ERROR")
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

    // Female
    val female_names = record_names
                        .select("gender", "first_name") // convert to RDD pair with gender and first_name columns
                        .where("gender = 'Female'") // gender filtering 
                        .as( (g: String, n: String) => (g,n) )

    val female_names_c = female_names.map{ case (k,v) => (v,1) } // associate 1 point to each male first name
                                     .cache // optimize several actions over RDD
    val females_result = female_names_c.reduceByKey{case (v,count) => count + count} // count 

    // taking 5 highest male repeated names                   
    val males_result_high = sc.parallelize(males_result.sortBy(_._2,false).take(5))

    // taking 5 highest female repeated names                   
    val females_result_high = sc.parallelize(females_result.sortBy(_._2,false).take(5))

    val highest = males_result_high
      .union(females_result_high)

    // taking all records with Top Five count for male and females
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

    // RDD join
    val vip_named = highest
      .join(pair_record_highest)
      .map{ case (name, ( count, row)) => row }

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

    // saving data into new table
    vip_named.saveToCassandra("examples","vip_named_people",SomeColumns("id","first_name","last_name","email","gender",
                              "birth_date","ip_address","probability","smoker_bool",
                              "drinker","language","image"))
    // finish
    sc.stop()
  }
}
