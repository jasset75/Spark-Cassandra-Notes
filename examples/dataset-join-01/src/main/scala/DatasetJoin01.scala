package uk.me.jasset.examples

// spark conf and context libraries
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// datastax Cassandra Connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.cql.CassandraConnector

// spark sql libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._

/**
 Computes an example joining two tables People with car owned
*/
object DatasetJoin01 {

  //app entry point
  def main(args: Array[String]) {
    
    // setting up Cassandra-ready spark session
    val spark = SparkSession
                  .builder()
                  .appName("SparkCassandraApp")
                  .config("spark.cassandra.connection.host", "localhost")
                  .config("spark.cassandra.connection.port", "9042")
                  .master("local[2]")
                  .getOrCreate()

    // setting error level to coherent threshold                
    spark.sparkContext.setLogLevel("ERROR")

    // db session stablishment
    val connector = CassandraConnector(spark.sparkContext.getConf)
    val session = connector.openSession()

    // reading datasets to join
    val dsPeople = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "mock_data", "keyspace" -> "examples")).load()
    val dsCars = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "mock_cars", "keyspace" -> "examples")).load()

    // joining datasets
    // for test only: it could be optimized filtering before join them
    val dsJoin = dsPeople.join(dsCars,dsPeople("id") <=> dsCars("id_owner"))

    // printing records of people who own blue honda cars 
    val dsBlueHonda = dsJoin.filter("color == 'Blue' and car_make == 'Honda'").select("email","id","car_model","drinker")
    println("People with a Blue Honda:")
    dsBlueHonda.show()

    // printing records of cars owned by people who daily drink :/
    val dsDrinkers = dsJoin.filter("drinker == 'Daily'").select("email","id","car_make","car_model")
    println("People and his cars who drink daily:")
    dsDrinkers.show(200,false)

    //finish
    session.close()
    spark.sparkContext.stop()
  }
}
