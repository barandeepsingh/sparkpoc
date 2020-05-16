package miniapps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

object OlympicsAnalyticsApp extends App {

  val spark = SparkSession.builder().appName("OlympicsAnalyticsApp").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  //Change it to the base dir where you keep the file
  val baseDirPath = "spark-data/"
  val readFilePath = baseDirPath + "olympics.csv"
  val exportFileDir = baseDirPath + "olympics_report"
  //Read CSV into DataSet
  val olympicsDataSet = spark.read.format("csv")
    .option("header", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .load(readFilePath).select("team", "year", "sport", "medal")
    .withColumn("year", 'year.cast(IntegerType))
    .as[Olympics]
  val olympicsTotalMedalsAllCountriesDataSet =
    olympicsDataSet
      .filter(olympicsDataSet.col("sport")
        .eqNullSafe("Swimming"))
      .filter(olympicsDataSet.col("medal")
        .notEqual("NA"))
      .groupBy("team")
      .count()
      .withColumnRenamed("count", "nom")
      .filter('nom >= 0)
      .sort(org.apache.spark.sql.functions.col("nom").desc)
  //Find the number of medals won by Canada year wise
  val medalsWonByIndiaYearlyDataSet =
    olympicsDataSet
      .filter('team === "India")
      .filter(olympicsDataSet.col("medal")
        .notEqual("NA"))
      .groupBy("year")
      .count()
      .withColumnRenamed("count", "nom")
      .filter('nom >= 0)
      .sort(org.apache.spark.sql.functions.col("nom").desc)


  //Find total number of medals won by each country in swimming
  olympicsTotalMedalsAllCountriesDataSet.show(60)
  val medalsWonByEachCountryYearlyDataSet =
    olympicsDataSet
      .filter(olympicsDataSet.col("medal")
        .notEqual("NA"))
      .groupBy("team")
      .count()
      .withColumnRenamed("count", "nom")
      .filter('nom >= 0)
      .sort(org.apache.spark.sql.functions.col("nom").desc)

  medalsWonByIndiaYearlyDataSet.show


  //Total no. of medals won by each country

  case class Olympics(team: String, year: Int, sport: String, medal: String)

  medalsWonByEachCountryYearlyDataSet.show(60, false)


}