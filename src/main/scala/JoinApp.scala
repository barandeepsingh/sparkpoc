import AirtelApp.spark
import org.apache.spark.sql.SparkSession

object JoinApp extends App {

  val sparkSession = SparkSession.builder().master("local[*]").appName("JoinApp").getOrCreate()

  import sparkSession.implicits._

  sparkSession.sparkContext.setLogLevel("INFO")
  println("Join App ")
  try {
    val employees = sparkSession.sparkContext.parallelize(Array[(String, Option[Int])](
      ("Rafferty", Some(31)), ("Jones", Some(33)), ("Heisenberg", Some(33)), ("Robinson", Some(34)), ("Smith", Some(34)), ("Williams", null)
    )).toDF("LastName", "DepartmentID")


    val departments = sparkSession.sparkContext.parallelize(Array(
      (31, "Sales"), (33, "Engineering"), (34, "Clerical"),
      (35, "Marketing")
    )).toDF("DepartmentID", "DepartmentName")

    //Inner Join
    // employees.join(departments,"DepartmentID").show

    //Left Outer Join
    employees.join(departments, Seq("DepartmentID"), "left_outer").show

    //Spark allows using following join types: inner, outer, left_outer, right_outer, leftsemi
  }
  catch {
    case ex: Exception => println("Exception occurred ")
  }
  finally {
    println("Finally Block")
  }
  spark.stop
}
