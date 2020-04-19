

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TestingSample extends App {
  val spark = SparkSession.builder().appName("TestingSample").master("local[*]").getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("FATAL")

  def showEntries(entries: RDD[String]) = {


    val myDF = entries.toDF

    myDF.show
  }

  showEntries(spark.sparkContext.parallelize(List("Item 1", "Item 2", "Item 3")))

}