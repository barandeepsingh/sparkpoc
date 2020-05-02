package miniapps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, row_number}


object SkipHeaders extends App {
  val spark = SparkSession.builder().appName("SkipHeaders").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("FATAL")

  val myDs = spark.read.csv("/Users/baran/Documents/spark-data/test_headers.csv")

  val myDsWithRowNumber = myDs.withColumn("row_number", row_number().over(Window.orderBy(lit(1)))).withColumnRenamed("_c0", "Id").withColumnRenamed("_c1", "Name").withColumnRenamed("_c2", "Age")

  myDsWithRowNumber.filter(myDsWithRowNumber.col("row_number") > 3).show()

}