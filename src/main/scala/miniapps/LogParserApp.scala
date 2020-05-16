package miniapps

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

case class LogRecord(clientIp: String, clientIdentity: String, user: String, dateTime: String, request: String, statusCode: Int, bytesSent: Long, referer: String, userAgent: String)


object LogParserApp extends App {


  //Set all configurations
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

  val sparkSession = SparkSession.builder().appName("LogParserApp").master("local[*]").getOrCreate()

  val log = Logger.getLogger("com.hortonworks.spark.Logs")
  log.info("Started Logs Analysis")


  sparkSession.conf.set("spark.cores.max", "16")
  sparkSession.conf.set("spark.serializer", classOf[KryoSerializer].getName)
  sparkSession.conf.set("spark.sql.tungsten.enabled", "true")
  sparkSession.conf.set("spark.eventLog.enabled", "true")
  sparkSession.conf.set("spark.app.id", "Logs")
  sparkSession.conf.set("spark.io.compression.codec", "snappy")
  sparkSession.conf.set("spark.rdd.compress", "false")
  sparkSession.conf.set("spark.shuffle.compress", "true")
  val logFile = sparkSession.sparkContext.textFile("spark-data/log_dump.log")


  Logger.getLogger("org").setLevel(Level.ERROR)
  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)"""".r
  val accessLogs = logFile.map(parseLogLine).filter(!_.clientIp.equals("Empty"))

  def parseLogLine(log: String): LogRecord = {
    try {
      val res = PATTERN.findFirstMatchIn(log)

      if (res.isEmpty) {
        println("Rejected Log Line: " + log)
        LogRecord("Empty", "-", "-", "", "", -1, -1, "-", "-")
      }
      else {
        val m = res.get
        // NOTE:   HEAD does not have a content size.
        if (m.group(9).equals("-")) {
          LogRecord(m.group(1), m.group(2), m.group(3), m.group(4),
            m.group(5), m.group(8).toInt, 0, m.group(10), m.group(11))
        }
        else {
          LogRecord(m.group(1), m.group(2), m.group(3), m.group(4),
            m.group(5), m.group(8).toInt, m.group(9).toLong, m.group(10), m.group(11))
        }
      }
    } catch {
      case e: Exception =>
        println("Exception on line:" + log + ":" + e.getMessage);
        LogRecord("Empty", "-", "-", "", "-", -1, -1, "-", "-")
    }
  }

  log.info("# of Partitions %s".format(accessLogs.partitions.size))

  try {
    println("===== Log Count: %s".format(accessLogs.count()))
    accessLogs.take(5).foreach(println)

    try {
      import sparkSession.implicits._

      val df1 = accessLogs.toDF()
      df1.createOrReplaceTempView("accessLogsDF")
      df1.printSchema()
      df1.describe("bytesSent").show()
      df1.first()
      df1.head()
      df1.explain()
      df1.show
      //df1.write.format("avro").mode(org.apache.spark.sql.SaveMode.Append).partitionBy("statusCode").avro("avroresults")
    } catch {
      case e: Exception =>
        log.error("Writing files after job. Exception:" + e.getMessage);
        e.printStackTrace();
    }

    // Calculate statistics based on the content size.
    val contentSizes = accessLogs.map(log => log.bytesSent)
    val contentTotal = contentSizes.reduce(_ + _)

    println("===== Number of Log Records: %s  Content Size Total: %s, Avg: %s, Min: %s, Max: %s".format(
      contentSizes.count,
      contentTotal,
      contentTotal / contentSizes.count,
      contentSizes.min,
      contentSizes.max))


    sparkSession.close()

  }
}