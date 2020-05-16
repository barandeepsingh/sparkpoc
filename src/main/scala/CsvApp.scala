import org.apache.spark.sql.SparkSession

object CsvApp extends App {

  println("CSV demo")
  val spark = SparkSession.builder().appName("PostgresApp").master("local[*]").getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("FATAL")

  val summerSlamRDD = spark.sparkContext.textFile("spark-data/summerslam.csv")
  val hellInACellRDD = spark.sparkContext.textFile("spark-data/hell_in_a_cell.csv")
  val summerSlamMappedRDD = summerSlamRDD.map { entry =>
    val cols = entry.split(",")
    PPV(challenger = cols(0), champion = cols(1), title = cols(2), "Summerslam")
  }
  val hellInACellMappedRDD = hellInACellRDD.map { entry =>
    val cols = entry.split(",")
    PPV(challenger = cols(0), champion = cols(1), title = cols(2), "Hell In A Cell")
  }
  val summerSlamDF = summerSlamMappedRDD.toDF.as[PPV]
  val HellInACellDF = hellInACellMappedRDD.toDF.as[PPV]
  val WweDF = summerSlamDF.union(HellInACellDF)

  case class PPV(challenger: String, champion: String, title: String, ppv: String)

  WweDF.show
  //summerSlamDF.select("champion","challenger").where("title like '%Universal%'").show


  //summerSlamDF.printSchema()

  //summerSlamDF.createOrReplaceTempView("summerSlam")

  //spark.sql("select * from summerSlam").select("challenger").where("champion='Kofi'").show


}
