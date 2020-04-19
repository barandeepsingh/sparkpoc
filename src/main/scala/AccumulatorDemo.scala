
import SocketStreamingApp.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object AccumulatorDemo extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder().appName("AccumulatorDemo").master("local[*]").getOrCreate()

  val data = sparkSession.sparkContext.textFile("/Users/baran/Documents/spark-data/wordcount.txt")

  val allFlattenedWords = data.flatMap(_.split("\\s+")).map(_.toLowerCase)

  val finalCollectedRdd = allFlattenedWords.map(entry => (entry, 1)).reduceByKey(_ + _)

  val countPresenceOfIf = sparkSession.sparkContext.doubleAccumulator("If accumulator")

  val countPresenceOfA = sparkSession.sparkContext.doubleAccumulator("A accumulator")

  finalCollectedRdd.foreach {
    line =>
      if (line.toString().toLowerCase().contains("if")) {
        countPresenceOfIf.add(1.0)
      } else if (line.toString().toLowerCase().contains("a")) {
        countPresenceOfA.add(1.0)
      }

  }
  println("Occurrence of if " + countPresenceOfIf.count)
  println("Occurrence of a " + countPresenceOfA.count)
  spark.close()
}
