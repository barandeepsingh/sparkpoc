package miniapps

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BucketingDemo extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().master("local[*]").appName("BucketingDemo").getOrCreate()
  val myRdd = spark.sparkContext.textFile("/Users/baran/Documents/spark-data/repartitioning.txt").filter(!_.contains("id"))

  import spark.implicits._

  val myDs = myRdd.map(processFile(_)).toDS().withColumn("day", dayofmonth($"timeSt")).withColumn("month", month($"timeSt")).withColumn("year", year($"timeSt")).withColumn("hour", hour($"timeSt"))

  def processFile(line: String): FakeData = {
    val fields = line.split("\t")

    FakeData(fields(0).toInt, fields(1), convertStringDateToTimeStamp(fields(2)))
  }

  def convertStringDateToTimeStamp(strDate: String): Timestamp = {
    val format = new SimpleDateFormat("yyyy-MM-dd' 'hh:mm:ss a")
    if (strDate.toString() == "")
      return null
    else {
      val d = format.parse(strDate);
      val t = new Timestamp(d.getTime());
      return t
    }

  }

  case class FakeData(id: Int, eventData: String, timeSt: Timestamp)

  myDs.write.
    //mode(SaveMode.Overwrite).bucketBy(2,"year").save("/Users/baran/Documents/spark-data/my_dump/bucket/")
    format("parquet")
    .bucketBy(4, "year", "month")
    .option("path", "/Users/baran/Documents/spark-data/my_dump/bucket/")
    .saveAsTable("myTable")
  myDs.printSchema()
  myDs.show()

  spark.stop()
}
