import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MapHashMapDemo extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("MapHashMapDemo").master("local[*]").getOrCreate()

  import spark.implicits._

  val myRdd = spark.sparkContext.textFile("spark-data/test.txt")

  myRdd.toDF.show

  val splittedMapRdd = myRdd.map(_.split(" "))

  splittedMapRdd.toDF.show

  val splittedFlatMapRdd = myRdd.flatMap(_.split(" "))

  splittedFlatMapRdd.toDF.show


  spark.close

}

