package uk.me.jasset.examples

// datastax Cassandra Connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.cql.CassandraConnector

// spark sql libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.SaveMode

/**
 Computes an example joining two tables People with car owned and save to specific Cassandra table
*/
object Test {

  //app entry point
  def main(args: Array[String]) {
    
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

    // setting error level to coherent threshold                
    spark.sparkContext.setLogLevel("ERROR")

    // db session stablishment
    val connector = CassandraConnector(spark.sparkContext.getConf)
    val session = connector.openSession()

    // reading datasets to join
    val dsPeople = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "mock_data", "keyspace" -> "examples"))
      .load()
      .filter("drinker == 'Daily'")

    // create table
    try {
      dsPeople.createCassandraTable(keyspaceName="examples_bis",tableName="drinkers")
    } catch {
      case e: Exception => 
    }


    dsPeople
      .write
      .mode(SaveMode.Append)
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "drinkers", "keyspace" -> "examples_bis"))
      .save()

    // showing
    println("Records saved.")
    
    val dsDrinkers = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "drinkers", "keyspace" -> "examples_bis"))
      .load()
      .show(200,false)

    // finish
    session.close()
  }
}
