package miniapps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}

object SkipHeaderAndFooter extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("SkipHeaderAndFooter")
      .config("spark.master", "local")
      .getOrCreate()

  import spark.implicits._

  val path = "spark-data/csv-corrupt-data.csv"
  val myRdd = spark.sparkContext.textFile(path).filter(_.contains(","))

  val countString = myRdd.take(1).mkString("")
  val commaCount = countString.count(_ == ',')
  println("String is " + countString)
  println("Length is " + commaCount)

  val myDF = myRdd.toDF
  myDF.withColumn("value", split(col("value"), ",")).select((1 until (commaCount+1)).map(i => col("value").getItem(i).as(s"col$i")): _*).show

  spark.close()
}