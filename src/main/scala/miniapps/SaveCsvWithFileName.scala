package miniapps

import io.delta.tables._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.File
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}

object SaveCsvWithFileName extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val startTime = LocalDateTime.now()

  val spark = SparkSession.builder()
    .appName("Save with filename")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val dumpFolderPath = "/tmp/test-table/"

  private val ratingsCsvDfTemp = spark.read
    .option("header", "true")
    .csv("/Users/baran/Downloads/5m-records.csv")

  private val ratingsCsvDf = ratingsCsvDfTemp.columns
    .foldLeft(ratingsCsvDfTemp)((curr, n) => curr.withColumnRenamed(n, n.replaceAll("\\s", "_")))

  ratingsCsvDf.write.format("delta").save("/tmp/delta-table")

  val deltaTable = DeltaTable.forPath("/tmp/delta-table")
  deltaTable.update(
    condition = expr("Country='India'"),
    set = Map("Country" -> expr("'Bharat'"))
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
  val endTime = LocalDateTime.now()
  val timeDiff = ChronoUnit.SECONDS.between(startTime, endTime)

  println("Processing completed in " + timeDiff + " seconds")
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
