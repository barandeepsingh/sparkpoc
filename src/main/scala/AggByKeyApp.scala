
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AggByKeyApp extends App {
  //AggByKey is a special case of reduceByKey where the return pair can be different (K,U) than input pair (K,V)

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder().appName("AggByKeyApp").master("local[*]").getOrCreate()

  val data = Seq("Amit=59", "Amit=80", "Anil=72", "Anil=77")

  val allData = sparkSession.sparkContext.parallelize(data, 2)

  val pairRdd = allData.map(x => x.split("=")).map(x => (x(0), x(1)))

  val initial = 0

  //val reducerOp=(sum:Int, newValue:String)=> sum +newValue.toInt
  val finalRdd = pairRdd.aggregateByKey(initial)((sum: Int, newValue: String) => sum + newValue.toInt, combinerOp)

  //val combinerOp= (x1:Int, x2:Int)=> x1+x2
  def combinerOp(x1: Int, x2: Int): Int = x1 + x2

  finalRdd.collect.foreach(println)

  sparkSession.close()
}
