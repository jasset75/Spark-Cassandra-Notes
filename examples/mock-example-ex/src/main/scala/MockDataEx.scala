package uk.me.jasset.examples

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/** Computes an example */
object MockDataEx {
  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "localhost")
    // spark context creation with conf
    val sc = new SparkContext(conf)
    // reducing verbosity
    sc.setLogLevel("ERROR")
    // select from Cassandra Table with Cassandra-Scala type conversion
    val record_names = sc.cassandraTable("examples","mock_data")
                        .cache
    //Male
    val male_names = record_names
                        .select("gender","first_name") //convert to RDD pair with gender and first_name columns
                        .where("gender = 'Male'") //gender filtering 
                        .as( (g: String, n: String) => (g,n) )

    val male_names_c = male_names.map{case (k,v) => (v,1)} //associate 1 point to each male first name
                                 .cache //optimize several actions over RDD
    val males_result = male_names_c.reduceByKey{case (v,count) => count + count} //count 
    //Female
    val female_names = record_names
                        .select("gender","first_name") //convert to RDD pair with gender and first_name columns
                        .where("gender = 'Female'") //gender filtering 
                        .as( (g: String, n: String) => (g,n) )

    val female_names_c = female_names.map{case (k,v) => (v,1)} //associate 1 point to each male first name
                                     .cache //optimize several actions over RDD
    val females_result = female_names_c.reduceByKey{case (v,count) => count + count} //count 

    // taking 5 highest male repeated names                   
    val males_result_high = males_result.sortBy(_._2,false).take(5)

    // taking 5 highest female repeated names                   
    val females_result_high = females_result.sortBy(_._2,false).take(5)

    val highest = males_result_high.union(females_result_high)
    // taking all records with Top Five count for male and females
    val record_highest = record_names
                           .where("first_name IN ?",highest.map(_._2).toList)
                           .as( (id: String, first_name: String, last_name: String,
                                 email:String, gender: String, ip_address: String,
                                 probability: Float, color: String, smoker_bool: Boolean,
                                 drinker: String, language: String, image: String) =>
                                (id,first_name,last_name,email,gender,ip_address,
                                 probability,color,smoker_bool,drinker,language,image) )

    record_highest.foreach(println)
    //finish
    sc.stop()
  }
}
