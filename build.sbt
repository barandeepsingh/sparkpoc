name := "SparkSamplesIJ"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.0"
libraryDependencies += "postgresql" % "postgresql" % "9.1-901-1.jdbc4"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.3"
//libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.6.0"
libraryDependencies += "com.univocity" % "univocity-parsers" % "2.8.3"
libraryDependencies += "com.twitter" % "hbc-core" % "2.2.0"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.1"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.2"
// https://mvnrepository.com/artifact/com.databricks/spark-xml
libraryDependencies += "com.databricks" %% "spark-xml" % "0.9.0"


