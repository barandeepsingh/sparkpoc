import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MovieApp extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder().master("local[*]").getOrCreate()


  import spark.implicits._

  val movieRdd = spark.sparkContext.textFile("spark-data/u.data").map(_.split("\t"))
  val movieDs = movieRdd.map(entry => {
    MovieRatings(entry(0).toInt, entry(2).toInt)
  }).toDS

  case class MovieRatings(movieId: Int, rating: Int)

  movieDs.createOrReplaceTempView("movieRatings")
  //movieDs.groupBy($"rating").agg()

  print("Default size is " + spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toInt)
  movieDs.show
  //spark.sql("""select rating, count(rating) from movieRatings group by rating order by rating desc""").show()
}
