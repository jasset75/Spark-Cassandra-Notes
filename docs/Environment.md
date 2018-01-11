# Setting up the Environment

### Description

The goal is to set up a development environment, just to try on the examples. And will be used to compute data from [Apache Cassandra](http://cassandra.apache.org/) (aka C*) repository.

C* NoSQL database which is an...

> "hybrid between a key-value and a column-oriented (or tabular) database management system"--- [Apache Cassandra. (2018, January 1). Wikipedia, The Free Encyclopedia.](https://en.wikipedia.org/wiki/Apache_Cassandra)

* Ubuntu 16.04.3 LTS (Xenial Xerus)
* Python 3.5.2
* Virtualenv 15.0.1
* Java 1.8.0
* Scala 2.11.6
* Spark 2.2.1
* Cassandra 3.11.1

### Preparatory

Update apt tool
```sh
$ sudo apt-get update
```

### Java 8 installation

```sh
$ sudo add-apt-repository -y ppa:webupd8team/java
$ sudo apt-get update
$ sudo apt-get install oracle-java8-installer
```

- version check
```sh
$ java -version
```

### Scala 2.11 installation
```sh
sudo apt-get install scala=2.11.6
```
- version check
```sh
$ scala -version
```

### Apache Spark 2.2.1 installation 
*(**with hadoop 2.7 support**)*
```sh
wget http://ftp.cixug.es/apache/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz
tar xvf spark-2.2.1-bin-hadoop2.7.tgz
sudo mv spark-2.2.1-bin-hadoop2.7 /usr/local/spark-2.2.1
```

### Adding to $PATH
```bs
echo "export SPARK_HOME=/usr/local/spark-2.2.1"
echo "export PATH=\$PATH:\$SPARK_HOME/bin" >> ~/.bashrc
```

### Python 3.5
```sh
$ sudo apt-get install python3
``` 

### Virtualenv
> Virtualenv is optional but highly recommended to follow the examples

```sh
$ cd $VIRTUALENVS_HOME
$ virtualenv -p `which python3` cassandra
$ source $VIRTUALENVS_HOME/cassandra/bin/activate
```
>`$VIRTUALENVS_HOME` is equal to whatever you create your virtual environments

- Script execution
```sh
(cassandra) $ cd ~/spark-cassandra-notes/examples/py-upload
(cassandra) $ pip install -r requirements.txt
(cassandra) $ python mock_data_imp.py 
``` 

### Jupyter 
*optional*

```sh
$ pip install findspark
$ pip install jupyter
````

### Datastax Spark-Cassandra Connector
> Source [Datastax Blog. (2018, January 1).](https://www.datastax.com/dev/blog/kindling-an-introduction-to-spark-with-cassandra-part-1)
```sh
$ git clone https://github.com/datastax/spark-cassandra-connector
$ cd spark-cassandra-connector
$ ./sbt/sbt -Dscala-2.11=true assembly
```

### Using spark-shell

Most times __*Spark Shell*__ is used in interactive mode. At other times, we can load script directly from command line, but each of them Spark Shell needs find his jars dependencies. In this case is about Datastax Cassandra Conector, previously compiled, we have to copy it into spark-shell search path. They usually are at `$SPARK_HOME/jars/`

```sh

$ cp ~/spark-cassandra-connector/spark-cassandra-connector/target/full/scala-2.11/spark-cassandra-connector-assembly-2.0.5-86-ge36c048.jar $SPARK_HOME/jars/
```


```sh
$ spark-shell --jars $SPARK_HOME/jars/spark-cassandra-connector-assembly-2.0.5-86-ge36c048.jar
```

Shell usage

```scala
// stop the Spark Context
scala> sc.stop

// library imports
scala> import com.datastax.spark.connector._
scala> import org.apache.spark.SparkContext
scala> import org.apache.spark.SparkContext._
scala> import org.apache.spark.SparkConf

// loading configuration
scala> val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
// new context 
scala> val sc = new SparkContext(conf)
```

### Using spark-submit

By this way we will able to launch packaged applications directly to the cluster. Scala applications (classes, resources, dependencies, etc.) need to be compiled into Java jars in order to be launched. So as precondition it is necessary setting up an Scala compiler like [sbt](https://www.scala-sbt.org/) and prepare your application in a [particular way](scala-app-template.md).

```sh
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get install sbt
```
