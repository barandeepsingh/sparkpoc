package miniapps

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LogParserDemo extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().master("local[*]").appName("LogParserDemo").getOrCreate()
  val logRdd = spark.sparkContext.textFile("/Users/baran/Documents/spark-data/log_dump.log").filter(countSubstring(_, ":-:") == 4)

  import spark.implicits._

  val logDs = logRdd.map(_.split(":-:")).map(entry => LogEntry(entry(0).trim.toString, entry(1).trim.toString, entry(2).trim.toString, entry(3).trim.toString, entry(4).toString)).toDS

  def countSubstring(str: String, sub: String): Int =
    str.sliding(sub.length).count(_ == sub)

  case class LogEntry(ip: String, userName: String, provider: String, date: String, url: String)

  logDs.groupBy("userName").pivot("ip").agg(concat_ws(" ", collect_list("provider")) as "providername").show

  spark.stop()
}
