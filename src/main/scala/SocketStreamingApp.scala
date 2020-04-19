import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object SocketStreamingApp extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("SocketStreamingApp").master("local[*]").getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("FATAL")

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))

  // Generate running word count
  val wordCounts = words.groupBy("value").count()

  // Start running the query that prints the running counts to the console
  val query = wordCounts.writeStream
    .outputMode(OutputMode.Update())
    .format("console")
    .start()

  query.awaitTermination()
  spark.close()
}
