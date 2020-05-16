
import SocketStreamingApp.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCountRDD extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder().appName("WordCountRDD").master("local[*]").getOrCreate()

  val data = sparkSession.sparkContext.textFile("spark-data/wordcount.txt")

  val allFlattenedWords = data.flatMap(_.split("\\s+")).map(_.toLowerCase)

  val finalCollectedRdd = allFlattenedWords.map(entry => (entry, 1)).reduceByKey(_ + _)

  val sortedRdd = finalCollectedRdd.map(_.swap).sortByKey(false).map(_.swap)

  //finalCollectedRdd.persist(StorageLevel.MEMORY_ONLY_SER_2)
  //println("Total Items " + finalCollectedRdd.count())
  sortedRdd.collect().take(3).foreach(entry => println(entry._1 + " : " + entry._2))
  //finalCollectedRdd.unpersist()
  spark.close()
}
