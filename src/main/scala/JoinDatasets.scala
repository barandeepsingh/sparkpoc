import org.apache.spark.sql.SparkSession

object JoinDatasets extends App {

  val sparkSession = SparkSession.builder().appName("JoinDataSets").master("local[*]").getOrCreate()

  import sparkSession.implicits._

  val employeeDataSet1 = sparkSession.sparkContext.parallelize(Seq(Employee("Max", 22, 1, 100000.0), Employee("Adam", 33, 2, 93000.0), Employee("Eve", 35, 2, 89999.0), Employee("Muller", 39, 3, 120000.0))).toDS()
  val employeeDataSet2 = sparkSession.sparkContext.parallelize(Seq(Employee("John", 26, 1, 990000.0), Employee("Joe", 38, 3, 115000.0))).toDS()
  val departmentDataSet = sparkSession.sparkContext.parallelize(Seq(Department(1, "Engineering"), Department(2, "Marketing"), Department(3, "Sales"))).toDS()
  val employeeDataset = employeeDataSet1.union(employeeDataSet2)
  val averageSalaryDataset = employeeDataset.joinWith(departmentDataSet, $"departmentId" === $"id", "inner")
    .map(record => Record(record._1.name, record._1.age, record._1.salary, record._1.departmentId, record._2.name))
    .filter(record => record.age > 25)
    .groupBy($"departmentId", $"departmentName")
    .avg()

  def averageSalary(key: (Int, String), iterator: Iterator[Record]): ResultSet = {
    val (total, count) = iterator.foldLeft(0.0, 0.0) {
      case ((total, count), x) => (total + x.salary, count + 1)
    }
    ResultSet(key._1, key._2, total / count)
  }

  case class Employee(name: String, age: Int, departmentId: Int, salary: Double)

  case class Department(id: Int, name: String)

  case class Record(name: String, age: Int, salary: Double, departmentId: Int, departmentName: String)

  case class ResultSet(departmentId: Int, departmentName: String, avgSalary: Double)

  averageSalaryDataset.show()
}
