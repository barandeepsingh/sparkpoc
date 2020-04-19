package miniapps


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCountApp extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder().appName("Spark - Word Count and Sort in Descending Order").master("local[*]").getOrCreate()

  import sparkSession.implicits._

  val data = sparkSession.read.text("/Users/baran/Documents/spark-data/wordcount.txt").as[String]

  val words = data.flatMap(value => value.split("\\s+"))

  val groupedWords = words.map(_.toLowerCase).map(entry => (entry, 1))
    .map(x => (x._1, x._2)) // [1]
    .groupByKey(_._1)
    .reduceGroups((a, b) => (a._1, a._2 + b._2))
    .map(_._2)
  // [2]
  println(groupedWords.show())
  // val reducedWords= groupedWords.rdd.reduceByKey(_)

  //val counts = groupedWords.count()

  //  print("Count is "+counts)

  sparkSession.close()

}
