import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession

object SparkStreamingKafkaApp extends App {
  println("CSV demo")
  val spark = SparkSession.builder().appName("SparkStreamingKafkaApp").master("local[*]").getOrCreate()


  spark.sparkContext.setLogLevel("FATAL")
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("topicA", "topicB")
  //  val stream = KafkaUtils.createDirectStream[String, String](
  //    streamingContext,
  //    PreferConsistent,
  //    Subscribe[String, String](topics, kafkaParams)
  //  )
  //
  //  stream.map(record => (record.key, record.value))


}
