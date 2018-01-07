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
 Computes an example joining two tables People with car owned and save to specific Cassandra table
*/
object DatasetJoin02 {

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

    // reading cars owned  
    val dsCars = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "mock_cars", "keyspace" -> "examples"))
      .load()

    // joining datasets
    val dsJoin = dsPeople
      .join(dsCars,dsPeople("id") <=> dsCars("id_owner"))
      .cache

    // select ok columns
    val dsDrinkers = dsJoin.select("id","email","car_id","car_make","car_model")
    // create table
    try {
      dsDrinkers
        .createCassandraTable("examples", "cars_owned_by_drinkers", partitionKeyColumns = Some(Seq("id","car_id")))
    } catch {
      case e: Exception => 
    }
    // write back to cassandra
    dsDrinkers
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "cars_owned_by_drinkers", "keyspace" -> "examples"))
      .save()

    println("People and his cars who drink daily:")
    
    val dsCarsDrinkers = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "cars_owned_by_drinkers", "keyspace" -> "examples"))
      .load()

    // showing
    println("Records saved")
    dsCarsDrinkers.show(200,false)

    // finish
    session.close()
  }
}
