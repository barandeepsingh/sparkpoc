
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CombineByKeyApp extends App {
  //AggByKey is a special case of reduceByKey where the return pair can be different (K,U) than input pair (K,V)

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkSession = SparkSession.builder().appName("CombineByKeyApp").master("local[*]").getOrCreate()

  val data = Seq("Amit=59", "Amit=80", "Anil=72", "Anil=77")

  val allData = sparkSession.sparkContext.parallelize(data, 2)

  val pairRdd = allData.map(x => x.split("=")).map(x => (x(0), x(1)))

  val initialCombiner = (count: Double) => (1, count)

  val partCombiner = (part: (Int, Double), count: Double) => part._1 + part._2 + count
  val partMerger = (part1: (Int, Double), part2: (Int, Double)) => (part1._1 + part2._1, part1._2 + part2._2)


  val reducerMethod = (sum: Int, newValue: String) => sum + newValue.toInt


  // val finalRdd = pairRdd.combineByKey(initialCombiner,partCombiner,partMerger)

  // finalRdd.collect.foreach(println)

  sparkSession.close()
}
