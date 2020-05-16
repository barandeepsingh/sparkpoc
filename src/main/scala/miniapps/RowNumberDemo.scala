package miniapps

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object RowNumberDemo extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder().master("local[*]").getOrCreate()


  import spark.implicits._

  val myRdd = spark.sparkContext.textFile("spark-data/duplicates.txt").map(_.split(" "))
  val myDs = myRdd.map(entry => {
    MyClass(entry(0).toInt, entry(1))
  }).toDS()
  val windowSpec = Window.partitionBy("id").orderBy("name")


  //  val myDs = Seq(("a", 10), ("a", 10), ("a", 20)).toDF("id", "name")

  case class MyClass(id: Int, name: String)

  myDs
    .withColumn("rank", rank().over(windowSpec))
    .withColumn("dense_rank", dense_rank().over(windowSpec))
    .withColumn("row_number", row_number().over(windowSpec)).show


  //myDs.createOrReplaceTempView("myView")

  //spark.sql("""select * from ( select *, row_number() over (partition by id order by name desc) as row_number from myView) where row_number=1""").show()
  //spark.sql("""select * from ( select *, rank() over (partition by id order by name desc) rank from myView)""").show()
  //spark.sql("""select * from (select *, dense_rank() over (partition by id order by name desc) dense_rank from myView)""").show()
}