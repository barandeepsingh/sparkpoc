import org.apache.spark.sql.SparkSession

object SampleRdd1 extends App {
  val session = SparkSession.builder().appName("SampleRdd1").master("local[2]").getOrCreate()

  val rdd1 = session.sparkContext.parallelize(Array("Scala", "Spark", "Java", "Python", "Go", "Django"))


  val finalRdd = rdd1.filter(_.startsWith("S"))

  println(finalRdd.collect.mkString(","))

  println("Hash of 223 is " + md5HashString("223"))

  def md5HashString(s: String): String = {
    import java.math.BigInteger
    import java.security.MessageDigest
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    hashedString
  }
}
