import org.apache.spark.sql.SparkSession


object PostgresApp extends App {
  println("Jdbc demo with Postgres")
  val spark = SparkSession.builder().appName("PostgresApp").master("local[*]").getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")
  val postgresDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/external_db")
    .option("dbtable", "product")
    .option("user", "postgres")
    .option("password", "postgres")
    .load().as[Product]
  //spark.sql("""select productid,productname from product where productid<4""").write.parquet("test.parquet")
  val parquetFileDF = spark.read.parquet("test.parquet").show()

  postgresDF.createOrReplaceTempView("product")

  case class Product(productid: BigInt, productname: String, userid: String)

}
