package miniapps

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PivotDemo extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession.builder().master("local[*]").appName("PivotDemo").getOrCreate()

  import spark.implicits._

  val myRdd = spark.sparkContext.parallelize(Seq(("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
    ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
    ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
    ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico")))

  val df = myRdd.toDF("Product", "Amount", "Country")
  df.show

  val pivotedDf = df.groupBy("Product").pivot("Country").sum("Amount").na.fill(0)

  pivotedDf.show

  spark.stop

}
