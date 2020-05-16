

import org.apache.spark.sql.SparkSession

object ReadCSV extends App {
  val spark = SparkSession.builder().appName("ReadCSV").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("FATAL")

  val myRdd = spark.sparkContext.textFile("spark-data/testcsv.csv")

  val splitRdd = myRdd.flatMap(_.split(",")).map(entry => entry.replaceAll("^\"|\"$", ""))

  splitRdd.foreach(println)

  val finalRdd = splitRdd.collect.mkString(",")

  println(finalRdd)
}