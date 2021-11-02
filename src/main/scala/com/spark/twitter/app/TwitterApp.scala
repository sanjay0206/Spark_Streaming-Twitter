package com.spark.twitter.app

import com.spark.twitter.config.TwitterConfig
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

object TwitterApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TwitterStreaming").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    sc.setLogLevel("ERROR")

    val filters = Array("Asymptomatic","Community spread","oxygen","cylinder",
      "Incubation period","Ventilator","Novel strain","Social distancing","Self-isolation","Self-quarantine",
      "Flatten the curve","Outbreak","Herd immunity","Surgical mask","N95 respirator","Epidemic","Pandemic")

    val auth = TwitterConfig.setCredentials()
    val twitterStream = TwitterUtils.createStream(ssc, Option(auth), filters)
    val filteredStream = twitterStream.filter(tweet => tweet.getLang.equals("en"))

    // Displaying Trending HashTags in Twitter
    println(TrendingHashTags.getTrendingHashTags(filteredStream))

    // SPARK SQL for twitter data
    val sqlContext = new SQLContext(sc)
    TwitterSparkSQL.performSparkSQL(filteredStream, sqlContext)

    ssc.start()
    ssc.awaitTermination()
    sc.stop();
  }
}
