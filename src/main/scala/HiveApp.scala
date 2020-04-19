import org.apache.spark.sql._

object HiveApp extends App {
  println("Spark demo with Hive")
  val spark = SparkSession.builder().appName("HiveApp").enableHiveSupport().master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  spark.sql("select * from employee").select("empName", "empId").sort("empName").show()

  spark.stop
}