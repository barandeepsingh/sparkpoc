package miniapps

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object RepartitionThenPartitionByDemo extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().master("local[*]").appName("RepartitionThenPartitionByDemo").getOrCreate()
  val myRdd = spark.sparkContext.textFile("spark-data/repartitioning.txt").filter(!_.contains("id"))

  import spark.implicits._

  val myDs = myRdd.map(processFile(_)).toDS().withColumn("day", dayofmonth($"timeSt")).withColumn("month", month($"timeSt")).withColumn("year", year($"timeSt")).withColumn("hour", hour($"timeSt")).repartition($"day", $"month", $"year").persist(StorageLevels.DISK_ONLY)

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

  myDs.write.partitionBy("year").mode(SaveMode.Overwrite).save("/Users/baran/Documents/spark-data/repartition_demo_dump/")
  myDs.printSchema()
  myDs.show()

  spark.stop()
}
