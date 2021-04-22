import org.apache.spark.sql.SparkSession

import java.util
import java.util.{Spliterator, Spliterators, StringJoiner}
import java.util.stream.{Collectors, StreamSupport}

object DataframeColumnAsString extends App {

  val spark = SparkSession.builder().appName("TestApp").master("local[*]").getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("FATAL")

  private val employeeNameIterator: util.Iterator[String] = spark.read
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .csv("spark-data/empdata.csv")
    .select("empid")
    .map(_.mkString)
    .toLocalIterator()


//  //1st way
//
//  var employeeIds = new StringJoiner(",")
//  while (employeeNameIterator.hasNext) {
//    employeeIds add employeeNameIterator.next
//  }
//
//  println("Using iterator with StringJoiner "+employeeIds)



  //2nd way

  val splitItr = Spliterators
    .spliteratorUnknownSize(employeeNameIterator, Spliterator.ORDERED)

  val employeeIdsUsingSpliterator = StreamSupport.stream(splitItr, false).collect(Collectors.joining(","))

  println("Using spliterator "+employeeIdsUsingSpliterator)


  //employeeNameIterator.
  //    .toLocalIterator()
  //    .forEachRemaining()


  spark.close()
}
