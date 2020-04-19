package miniapps

import org.apache.spark.sql.SparkSession

object ProcessInsuranceCsvApp extends App {

  val spark = SparkSession.builder().appName("ProcessInsuranceCsvApp").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

  //Change it to the base dir where you keep the file
  val baseDirPath = "/Users/baran/Downloads/"
  val readFilePath = baseDirPath + "Insurance Form.csv"
  val exportFileDir = baseDirPath + "Insurance Form Processed"

  //Read CSV into DataSet
  val insuranceFormsWithSchemaDataSet = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("timestampFormat", "dd/MM/yy HH:mm")
    .option("treatEmptyValuesAsNulls", "true")
    .load(readFilePath).drop("E")
    .withColumnRenamed("Modified Date", "modifiedDate")

  //Create temp view to use Spark SQL
  insuranceFormsWithSchemaDataSet.createOrReplaceTempView("insurance_temp_view")

  //Repartition so that all parts are combined and persist to disk

  spark.sql("Select * from (SELECT *,ROW_NUMBER() OVER (PARTITION BY ECODE ORDER BY modifiedDate DESC) AS rn FROM insurance_temp_view) where rn=1")
    .na.fill("")
    .drop("rn")
    .coalesce(1)
    .write
    .option("header", "true")
    .option("sep", ",")
    .mode("overwrite")
    .csv(exportFileDir)

  println("File exported to directory " + exportFileDir)


}
