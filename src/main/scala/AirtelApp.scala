
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

object AirtelApp extends App {

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val rddLog = spark.sparkContext.textFile("/Users/baran/Documents/spark-data/airtel.log")
  val myDS = rddLog.map(_.split(" ")).map(entry => {
    LogDetails(entry(4), entry(5), Timestamp.valueOf(entry(0) + " " + entry(1)), getCurrentdateTimeStamp, entry(6))
  }).toDS

  def getCurrentdateTimeStamp: Timestamp = {
    val date = new java.util.Date
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    java.sql.Timestamp.valueOf(sdf.format(date))
  }

  //val myDf = spark.read.csv("spark-data/airtel.log")
  case class LogDetails(userId: String, sessionId: String, timestamp: Timestamp, currentTimestamp: Timestamp, status: String)

  myDS.createOrReplaceTempView("airtel")
  //spark.sql("""select datediff(currentTimestamp,timestamp) as timestamp_diff from airtel where userId=1""").show
  spark.sql("""select * from airtel""").show


}