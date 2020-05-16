
import SocketStreamingApp.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object BroadCastVariableDemo extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder().appName("BroadCastVariableDemo").master("local[*]").getOrCreate()

  val data = sparkSession.sparkContext.textFile("spark-data/wordcount.txt")

  val allFlattenedWords = data.flatMap(_.split("\\s+")).map(_.toLowerCase)

  val finalCollectedRdd = allFlattenedWords.map(entry => (entry, 1)).reduceByKey(_ + _)

  val dictionary = Map(1 -> "a", 1 -> "an", 3 -> "the")

  val broadcastDict = sparkSession.sparkContext.broadcast(dictionary)

  finalCollectedRdd.map(x => x._1 + " , " + x._2 + " , " + broadcastDict.value.get(x._2).getOrElse("n/a")).take(10).foreach(println)

  broadcastDict.unpersist()
  spark.close()
}
