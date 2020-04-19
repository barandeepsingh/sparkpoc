package miniapps

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object XmlParserApp extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  Logger.getLogger("org").setLevel(Level.OFF)

  val df = spark.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "person")
    .load("/Users/baran/Documents/spark-data/persons.xml")


  df.printSchema()

  df.createOrReplaceTempView("personsView")
  spark.sql("""select _id,dob_month,salary._VALUE as salary_amount,salary._currency as salary_currency from personsView""").show()
  spark.close()
}
