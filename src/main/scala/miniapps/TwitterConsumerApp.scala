package miniapps

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder


object TwitterConsumerApp extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("TwitterConsumerApp").master("local[*]").getOrCreate()

  val sc = spark.sparkContext
  //create context
  val ssc = new StreamingContext(sc, Seconds(10))

  val consumerKey = "FyxIpNvlAregYrmRJKdMRGn0f"
  val consumerSecret = "9EUPdPXi1WzKf6NTwryWtHlt8Ffk8965bdNkJCOghHim8q1IIO"
  val accessToken = "91073134-ou7DEOMtcnutzjiEADzUmPCwdotVRMJ1LzvzH4Wc4"
  val accessTokenSecret = "RkiZYbZ0d7TmonPAB3fpZgw2vniG1IgYvmMuxPDgJkqdL"


  System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
  System.setProperty("twitter4j.oauth.accessToken", accessToken)
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


  //Connection to Twitter API
  val cb = new ConfigurationBuilder
  cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)

  val auth = new OAuthAuthorization(cb.build)
  val tweets = TwitterUtils.createStream(ssc, Some(auth))
  val englishTweets = tweets.filter(_.getLang() == "en")

  val statuses = englishTweets.map(status => (status.getText(), status.getUser.getName(), status.getUser.getScreenName(), status.getCreatedAt.toString))


  statuses.foreachRDD { (rdd, time) =>

    rdd.foreachPartition { partitionIter =>
      val props = new Properties()
      val bootstrap = "localhost:9092"
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("bootstrap.servers", bootstrap)
      val producer = new KafkaProducer[String, String](props)
      partitionIter.foreach { elem =>
        val dat = elem.toString()
        if (dat.contains("#")) {
          val myRdd = sc.parallelize(dat.split(" ").filter(_.startsWith("#")).map(entry => (entry, 1)))
          myRdd.reduceByKey(_ + _).map(_.swap).sortByKey(false).map(_.swap).take(20).foreach(println(_))
          //foreach(entry=>if(!entry.contains("#")){println(entry)})

        }
        //dat.split(" ").filter(!_.contains("#")).foreach(println(_))

        //val data = new ProducerRecord[String, String]("twitter_topic", null, dat)
        //producer.send(data)
      }
      producer.flush()
      producer.close()
    }
  }
  ssc.start()
  ssc.awaitTermination()
}
