package miniapps

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.explode

object JsonParserApp extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  Logger.getLogger("org").setLevel(Level.OFF)

  val df = spark.read.option("multiLine", true).json("spark-data/nested.json")

  df.show()

  val dfDates = df.select(explode(df("content"))).toDF("content")
  println("dfDates")
  dfDates.printSchema
  println("dfFooBar")
  val dfFooBar = dfDates.select("content.foo", "content.bar")

  dfFooBar.show()
  spark.close()
}
