import org.apache.spark.sql.SparkSession

object NestedJsonApp extends App {

  println("Nested Json demo")
  val spark = SparkSession.builder().appName("NestedJsonApp").master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("FATAL")

  val jsonDf = spark.read.option("multiline", true).json("")


}
