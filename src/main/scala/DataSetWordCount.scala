import org.apache.spark.sql.SparkSession

object DataSetWordCount extends App {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("DataSetWordCount")
    .getOrCreate()

  import sparkSession.implicits._

  val data = sparkSession.read.text("spark-data/wordcount.txt").as[String]


  val words = data.flatMap(value => value.split("\\s+"))

  val groupedWords = words.groupByKey(_.toLowerCase)

  val counts = groupedWords.count.withColumnRenamed("value", "word").withColumnRenamed("count(1)", "occurence")

  counts.show
}
