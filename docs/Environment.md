# Setting up the Environment

## Description

The goal is to set up a development environment. This will be used to compute data from [Apache Cassandra](http://cassandra.apache.org/) (aka C*) repository.

C* NoSQL database which is an...

> "hybrid between a key-value and a column-oriented (or tabular) database management system"
--- [Apache Cassandra. (2018, January 1). Wikipedia, The Free Encyclopedia.](https://en.wikipedia.org/wiki/Apache_Cassandra)

* Ubuntu 16.04.3 LTS (Xenial Xerus)
* Python 3.5.2
* Virtualenv 15.0.1
* Java 1.8.0
* Scala 2.11.6
* Spark 2.2.1
* Cassandra 3.11.1

## Preparatory

Update apt tool
```sh
$ sudo apt-get update
```

## Java 8 installation

```sh
$ sudo add-apt-repository -y ppa:webupd8team/java
$ sudo apt-get update
$ sudo apt-get install oracle-java8-installer
```
- version check
```sh
java -version
```

## Scala 2.11 installation
```sh
sudo apt-get install scala
```
- version check
```sh
scala -version
```

## Apache Spark 2.2.1 installation with hadoop 2.7 support
```sh
wget http://ftp.cixug.es/apache/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz
tar xvf spark-2.2.1-bin-hadoop2.7.tgz
sudo mv spark-2.2.1-bin-hadoop2.7 /usr/local/spark-2.2.1
```

## adding to path
echo "export SPARK_HOME=/usr/local/spark-2.2.1"
echo "export PATH=\$PATH:\$SPARK_HOME/bin" >> ~/.bashrc
```

## Python 3.5
```
$ sudo apt-get install python3
$ mkdir ~/project
$ virtualenv -p `which python3` pyspark
$ source ./pyspark/bin/activate
``` 

## Jupyter (if necessary)
$pip install findspark
$pip install jupyter

## Datastax Spark-Cassandra Connector
> Source 
--- [Datastax Blog. (2018, January 1).](https://www.datastax.com/dev/blog/kindling-an-introduction-to-spark-with-cassandra-part-1)

```sh
$ git clone https://github.com/datastax/spark-cassandra-connector
$ cd spark-cassandra-connector
$ ./sbt/sbt -Dscala-2.11=true assembly
```

## spark-shell environment

Most times Spark Shell is used in interactive mode. Spark Shell need load jars with
dependencies; in this case Datastax Cassandra Conector, previously compiled.

```sh
$ cp ~/spark-cassandra-connector/spark-cassandra-connector/target/full/scala-2.11/spark-cassandra-connector-assembly-2.0.5-86-ge36c048.jar /usr/local/spark-2.2.1/jars/
```

```sh
$ spark-shell --jars $SPARK_HOME/jars/spark-cassandra-connector-assembly-2.0.5-86-ge36c048.jar
```

shell commands

```scala
sc.stop
import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
val sc = new SparkContext(conf)
```

## spark-submit environment

In this mode, you could launch applications directly to the cluster. Scala application need to be compiled into Java jars in order to be launched. So as precondition it is necessary setting up an Scala compiler like [sbt](https://www.scala-sbt.org/).

```sh
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get install sbt
```