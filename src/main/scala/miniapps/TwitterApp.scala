package miniapps

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder


object TwitterApp extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("TwitterApp").master("local[*]").getOrCreate()

  val sc = spark.sparkContext
  //create context
  val ssc = new StreamingContext(sc, Seconds(10))

  val consumerKey = ""
  val consumerSecret = ""
  val accessToken = ""
  val accessTokenSecret = ""


  System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
  System.setProperty("twitter4j.oauth.accessToken", accessToken)
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


  //Connection to Twitter API
  val cb = new ConfigurationBuilder
  cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)

  val auth = new OAuthAuthorization(cb.build)
  val tweets = TwitterUtils.createStream(ssc, Some(auth))

  val tweetsData = tweets.map(tweet => tweet.getText)

  val hbgAuthorsRdd = sc.parallelize(List("iHeart", "BellLetsTalk", "BTSARMY", "Rashami", "CaptainCool", "Big"))

  val hbgAuthors = hbgAuthorsRdd.collect.toList

  val onlyHashtags = tweetsData.flatMap(_.split(" ")).filter(_.startsWith("#")).filter(authorName => hbgAuthors.contains(authorName.substring(1))) //.filter(_.startsWith("#")).map((_,1))

  //val hashTagCounts = onlyHashtags.reduceByKeyAndWindow((_ + _), (_ - _), Seconds(300), Seconds(10))

  val hashTagCounts = onlyHashtags.countByValueAndWindow(Seconds(300), Seconds(10))

  val sortedResults = hashTagCounts.transform(rdd => rdd.sortBy(_._2, false))

  sortedResults.print(20)


  //sortedResults.filter(tweet=>hbgAuthors.contains(tweet._1.substring(1))).foreachRDD(println(_))


  ssc.checkpoint("/Users/baran/Documents/checkpoints/")
  ssc.start()

  ssc.awaitTermination()
}
