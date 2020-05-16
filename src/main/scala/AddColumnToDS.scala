import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object AddColumnToDS extends App {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val movieRdd = spark.sparkContext.textFile("spark-data/u.data").map(_.split("\t"))
  val movieDs = movieRdd.map(entry => {
    MovieRatings(entry(0).toInt, entry(2).toInt)
  }).toDS
  //Anonymous function
  val showName: (Int) => String = (movieId: Int) => {

    movieId match {
      case x if x % 2 == 0 => "Baran"
      case x if x % 2 != 0 => "Simran"
    }
  }

  //movieDs.createOrReplaceTempView("movieRatings")
  //movieDs.groupBy($"rating").agg()
  //Declare the UDF
  val addColumnUDF = udf(showName)

  case class MovieRatings(movieId: Int, rating: Int)

  //Add the new column by calling the udf
  movieDs.withColumn("reviewer", addColumnUDF(movieDs.col("movieId"))).show

  //movieDs.withColumn("reviewer", showName('movieId) ).show


  //spark.sql("""select rating, count(rating) from movieRatings group by rating order by rating desc""").show()
}
