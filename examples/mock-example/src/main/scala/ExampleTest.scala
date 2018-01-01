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
    //select from Cassandra Table with Cassandra-Scala type conversion
    sc.cassandraTable[(String,String)]("examples","mock_data")
      .where("gender = 'Male'") //gender filtering
      .select("gender","first_name") //conver to RDD pair with gender and first_name columns
      .map{case (k,v) => (v,1)} //associate 1 point to each male first name
      .reduceByKey{case (v,count) => count + count} //count 
      .sortByKey
      .collect
      .foreach(println)
    sc.stop()
  }
}





