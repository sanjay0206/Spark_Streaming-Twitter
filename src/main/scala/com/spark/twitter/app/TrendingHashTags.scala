package com.spark.twitter.app

import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status


object TrendingHashTags {

  def getTrendingHashTags(filteredStream: DStream[Status]): Any = {
    // Fetching top 10 tweets for last one day
    val hashTags = filteredStream

      .flatMap(tweet => tweet.getText.split(" ")
        .filter(_.startsWith("#")))

    val top10TweetsInLastOneDay = hashTags.map((_, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(86400))
      .map({ case (hashTag, tagCount) => (tagCount, hashTag) })
      .transform(_.sortByKey(ascending = false))

    top10TweetsInLastOneDay.foreachRDD(rdd => {
      val top10 = rdd.take(10)
      println("\nPopular tweets related to covid for past 1 day (out of %s total tweets)".format(rdd.count()))
      top10.foreach({ case (tagCount, hashTag) => println("(%s, %s hashtags)".format(hashTag, tagCount))} )
    })

  }
}
