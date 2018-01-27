[< Back Home](./)

# Setting up the Environment

> **“There are not more than five musical notes, yet the combinations of these five give rise to more melodies than can ever be heard.**

> **There are not more than five primary colours, yet in combination they produce more hues than can ever been seen.**

> **There are not more than five cardinal tastes, yet combinations of them yield more flavours than can ever be tasted.”**  ---[Sun Tzu, The Art of War](https://en.wikipedia.org/wiki/The_Art_of_War)

### Description

The goal is to set up a development environment, just to try a sort of examples. This examples will be used to compute data from [Apache Cassandra](http://cassandra.apache.org/) (aka C*) repository into Apache Spark. So then, we need to combine different pieces of software.

Apache Cassandra could be defined as:
> "hybrid between a key-value and a column-oriented (or tabular) database management system"--- [Apache Cassandra. (2018, January 1). Wikipedia, The Free Encyclopedia.](https://en.wikipedia.org/wiki/Apache_Cassandra)

We need an environment which be able to connect to Cassandra cluster and extract data from that without _bottle necks_ if possible. In this environment we'll install Cassandra locally. However, it could access to a remote cluster as well.

We could use several programming languages onto Spark; Scala is one of them, and it has Java Virtual Machine as a requirement, so we need to install Java + Scala in order to use Spark with, as well as python interpreter if we use this programming language.

Spark is scalable; so, developing a cluster of nodes we could use pararell programming achiving the optimun. But for simplicity sake, we'll also install spark locally. Spark also take advantage from multiple core or threads as well as GPU with specific conditions (i.e.: using [CUDA](http://www.spark.tc/gpu-acceleration-on-apache-spark-2/))

As development environment we will use an VMWare Virtual Machine with Ubuntu as O.S.; this is the basic list of tuples product-version we are going to harness:

* Ubuntu 16.04.3 LTS (Xenial Xerus)
* Python 3.5.2
* Virtualenv 15.0.1
* Java 1.8.0
* Scala 2.11.6
* sbt 0.13
* Spark 2.2.1
* Cassandra 3.11.1

### Preparatory

Update apt tool
```sh
$ sudo apt-get update
```

### Java 8 installation

~~$ sudo apt-get install software-properties-common~~

~~$ sudo add-apt-repository -y ppa:webupd8team/java~~

~~$ sudo apt-get update~~

~~$ sudo apt-get install oracle-java8-installer~~

>Note: Since January 2018, Oracle has discontinued JDK 8 binaries support, so I'll try OpenJDK 8 instead of Oracle JDK 9, because I've read some issues with.

```sh
sudo apt-get install openjdk-8-jdk
```

> version check
```sh
$ java -version
```

### Scala 2.11 installation
```sh
wget www.scala-lang.org/files/archive/scala-2.11.6.deb
sudo dpkg -i scala-2.11.6.deb
```

> version check
```sh
$ scala -version
```
### Cassandra 3.11 installation
```sh
$ echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
$ sudo apt-get install curl
$ curl https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -
$ sudo apt-key adv --keyserver pool.sks-keyservers.net --recv-key A278B781FE4B2BDA
$ sudo apt-get update
$ sudo apt-get install cassandra 
```
> version check `$ cassandra -v`

### Apache Spark 2.2.1 installation 
*(**with hadoop 2.7 support**)*
```sh
$ wget http://ftp.cixug.es/apache/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz
$ tar xvf spark-2.2.1-bin-hadoop2.7.tgz
$ sudo mv spark-2.2.1-bin-hadoop2.7 /usr/local/spark-2.2.1
```

### Adding to $PATH environment variable
```bs
$ echo "export SPARK_HOME=/usr/local/spark-2.2.1" >> ~/.bashrc
$ echo "export PATH=\$PATH:\$SPARK_HOME/bin" >> ~/.bashrc
$ source ~/.bashrc
```

### sbt 0.13 installation

> To compile scala applications we need i.e. sbt compiler

```sh
$ echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
$ sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
$ sudo apt-get update
$ sudo apt-get install sbt
```

### Python 3.5

```sh
$ sudo apt-get install python3
``` 

### Virtualenv

> Virtualenv is optional but highly recommended to follow the examples

```sh
$ sudo apt-get virtualenv
$ echo "export VIRTUALENVS_HOME=/usr/local/spark-2.2.1" >> ~/.bashrc
$ cd $VIRTUALENVS_HOME
$ virtualenv -p `which python3` cassandra
```
>`$VIRTUALENVS_HOME` is equal to whatever you create your virtual environments

> Virtual environment activation `$ source $VIRTUALENVS_HOME/cassandra/bin/activate`

Script execution

> cloning this repository `$ git clone https://github.com/jasset75/Spark-Cassandra-Notes.git spark-cassandra-notes`

```sh
(cassandra) $ cd ~/spark-cassandra-notes/examples/py-upload
(cassandra) $ pip install -r requirements.txt
(cassandra) $ python mock_data_imp.py 
``` 

> `requirements.txt` has python libraries that are dependencies for all the examples

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

Most times __*Spark Shell*__ is used in interactive mode. At other times, we can load script directly from command line but each of them, Spark Shell needs find his jars dependencies. In this case we refer to Datastax Cassandra Connector which was previously compiled and, It have to be copied into spark-shell search path. They usually are at `$SPARK_HOME/jars/`

```sh
$ cp ~/spark-cassandra-connector/spark-cassandra-connector/target/full/scala-2.11/spark-cassandra-connector-assembly-2.0.5-86-ge36c048.jar $SPARK_HOME/jars/
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
$ echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
$ sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
$ sudo apt-get update
$ sudo apt-get install sbt
```

Configure `build.sbt`

```
name := 'app'
version := "1.0"

scalaVersion := "2.11.6"

// spark version which fit with the app
val sparkVersion = "2.2.1"

// external dependencies i.e.: Spark-Cassandra Connector
unmanagedJars in Compile += file("lib/spark-cassandra-connector.jar")

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

// managed dependencies (ivy, maven, etc.)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)
```

At root folder of your application repository:

```sh
$ cd app
$ ls
build.sbt  lib  src
$ sbt package
$ ls
build.sbt  lib  project  src  target
$ ls target/scala-2.11/
classes  app_2.11-1.0.jar  resolution-cache
```

These commands generate a jar package within the target folder ([see de schema](scala-app-template.md)). For instance, `target/scala-2.11/app_2.11-1.0.jar`. So, if you want launch the new compiled application you could use:

```sh
$ spark-submit target/scala-2.11/app_2.11-1.0.jar
```
