import org.apache.spark.sql.SparkSession

object Difference extends App {
  println("Spark demo with Hive")
  val spark = SparkSession.builder().appName("DifferenceApp").enableHiveSupport().master("local[*]").getOrCreate()

  import spark.implicits._

  val df1 = spark.sparkContext.parallelize(List("c", "c", "p", "m", "t")).toDS
  val df2 = spark.sparkContext.parallelize(List("c", "m", "k")).toDS
  //df1.union(df2).subtract(df1.intersect(df2))
  df1.union(df2).except(df1.intersect(df2)).show
  //spark.sparkContext.parallelize(List("c", "c", "p", "m", "t")).toDS.createOrReplaceTempView("view1")
  //spark.sparkContext.parallelize(List("c", "m", "k")).toDS.createOrReplaceTempView("view2")

  /* val result = spark.sql(
    """ select *
    from view1 v1
    where L.some_identifier NOT IN (
            select S.some_identifier
            from smallDataFrame S
        )""")

  result.collect


   */
}