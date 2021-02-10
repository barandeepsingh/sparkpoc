package miniapps

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import io.delta.tables._
import org.apache.commons.io.FileUtils

import java.io.File

object SaveCsvWithFileName extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName("Save with filename")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val dumpFolderPath = "/tmp/test-table/"

  private val ratingsCsvDf: DataFrame = spark.read
    .option("header", "true")
    .csv("spark-data/ratings.csv")


  ratingsCsvDf.write.format("delta").save("/tmp/delta-table")
  val deltaTable = DeltaTable.forPath("/tmp/delta-table")
  deltaTable.update(
    condition = expr("rating>3"),
    set = Map("movieId" -> expr("123"))
  )

  deltaTable.toDF
    .coalesce(1)
    .write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", ",")
    .save(dumpFolderPath)


  getListOfFiles(dumpFolderPath)
    .filter(_.getName.endsWith(".csv"))
    .foreach(FileUtils.moveFile(_, new File(dumpFolderPath + "myCsv.csv")))

  deltaTable.delete()


  println("Processing completed")
  spark.close()

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}
