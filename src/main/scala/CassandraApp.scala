
import org.apache.spark.sql.SparkSession

object CassandraApp extends App {
  println("Spark demo with Cassandra")
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  import spark.implicits._

  spark.conf.set("spark.cassandra.connection.host", "127.0.0.1")
  spark.conf.set("spark.cassandra.connection.port", "9042")

  spark.sparkContext.setLogLevel("WARN")

  /* Code for storing data in Cassandra -- start */
  val cassandraDF = spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "survey_results", "keyspace" -> "sparkdb"))
    .load.as[SurveyResult]

  /* val females = new SurveyResult("female", 13, 14)
  val transGender = new SurveyResult("trans gender", 1, 4)
  val saveToCassDF = Seq(females, transGender).toDF
  saveToCassDF.write
    .mode("append")
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "survey_results", "keyspace" -> "sparkdb"))
    .save()
  println("Data saved in Cassandra successfully")
*/
  /* Code for storing data in Cassandra -- end */

  case class SurveyResult(gender: String, sum_no: Int, sum_yes: Int)

  cassandraDF.explain
  cassandraDF.show

}