package uk.me.jasset.examples

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/** Computes an example */
object MockData {
  def main(args: Array[String]) {
    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "localhost")
    // spark context creation with conf
    val sc = new SparkContext(conf)
    // reducing verbosity
    sc.setLogLevel("ERROR")
    // select from Cassandra Table with Cassandra-Scala type conversion
    val record_names = sc.cassandraTable[(String,String)]("examples","mock_data")
                        .select("gender","first_name") //convert to RDD pair with gender and first_name columns              
                        .cache
    //Male
    val male_names = record_names.where("gender = 'Male'") //gender filtering 
    val male_names_c = male_names.map{case (k,v) => (v,1)} //associate 1 point to each male first name
              .cache //optimize several actions over RDD
    val males_result = male_names_c.reduceByKey{case (v,count) => count + count} //count 
    //Female
    val female_names = record_names.where("gender = 'Female'") //gender filtering 
    val female_names_c = female_names.map{case (k,v) => (v,1)} //associate 1 point to each male first name
              .cache //optimize several actions over RDD
    val females_result = female_names_c.reduceByKey{case (v,count) => count + count} //count 
    //Male
    println("Ordered Male Names count list:")
    // ordered RDD by male names                            
    val males_result_az = males_result.sortByKey() //key male names are sorted in asc order
    males_result_az.collect.foreach(println) //print result records through stdout
    // taking 5 highest male repeated names                   
    println("Five highest repeated male names:")
    val males_result_high = males_result.sortBy(_._2,false).take(5)
    males_result_high.foreach(println)
    //Female
    println("Ordered Female Names count list:")
    // ordered RDD by female names                            
    val females_result_az = females_result.sortByKey() //key male names are sorted in asc order
    females_result_az.collect.foreach(println) //print result records through stdout
    // taking 5 highest female repeated names                   
    println("Five highest repeated female names:")
    val females_result_high = females_result.sortBy(_._2,false).take(5)
    females_result_high.foreach(println)
    sc.stop()
  }
}
