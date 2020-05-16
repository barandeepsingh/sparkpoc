import org.apache.spark.sql.SparkSession

object MoviesDbCsvApp extends App {

  println("CSV demo")
  val spark = SparkSession.builder().appName("MoviesDbCsvApp").master("local[*]").getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("FATAL")
  val ratingsDS = spark.read.option("header", "true").option("inferSchema", "true").csv("/Users/baran/Documents/ml-lib-data/ratings.csv").as[Movie]

  case class Movie(userId: Int, movieId: Int, rating: Double)

  //ratingsDS.printSchema

  //print(ratingsDS.filter("rating==4.0").where("userId==1").count)
  print(ratingsDS.where("movieId==119050").count)

  //summerSlamDF.createOrReplaceTempView("summerSlam")

  //spark.sql("select * from summerSlam").select("challenger").where("champion='Kofi'").show

}
