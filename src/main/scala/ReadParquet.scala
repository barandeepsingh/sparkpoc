

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ReadParquet extends App {
  val spark = SparkSession.builder().appName("ReadParquet").master("local[*]").getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR)

  val myDs = spark.read.parquet("spark-data/my_dump")

  myDs.printSchema()

  myDs.show

}