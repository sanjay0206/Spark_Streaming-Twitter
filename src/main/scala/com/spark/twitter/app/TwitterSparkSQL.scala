package com.spark.twitter.app

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

import scala.collection.mutable

object TwitterSparkSQL {

  def performSparkSQL(filteredStream: DStream[Status], sqlContext: SQLContext): Unit = {

    // defining schema
    val schemaString = "user tweet retweets account_type followers location tweet_time"
    val fields = schemaString.split(" ")
      .map(field => {
        if (field.contentEquals("retweets") || field.contentEquals("followers")) {
          StructField(field, IntegerType, nullable = false)
        } else {
          StructField(field, StringType, nullable = false)
        }
      })

    // creating schema
    val schema = StructType(fields)
    filteredStream.foreachRDD(rdd => {
      val newRDD = rdd.map(tweet => {
        val user = "@" + tweet.getUser.getScreenName
        val tweetedLine = tweet.getText
        val tweetedTime = tweet.getUser.getCreatedAt.toInstant.toString
        val retweets = tweet.getRetweetCount
        val accountType = if (tweet.getUser.isVerified) "Verified Account" else "Not Verified"
        val followers = tweet.getUser.getFollowersCount
        val location = if (tweet.getUser.getLocation == null) "Location is disabled" else tweet.getUser.getLocation

        // store the data into case class
        case class TwitterDataset(user: String, tweet: String, retweets: Integer, accountType: String,
                                  followers: Integer, location: String, tweetedTime: String)

        val twitterDataset = TwitterDataset(user, tweetedLine, retweets, accountType, followers, location, tweetedTime)
        val twitterDatasetMap = new mutable.HashMap[String, TwitterDataset]()
        twitterDatasetMap.put("Result", twitterDataset)
        println(twitterDataset)

        // Creating Row in dataframe
        Row(user, tweetedLine, retweets, accountType, followers, location, tweetedTime)
      })

      // Creating dataframe
      val df = sqlContext.createDataFrame(newRDD, schema)
      df.printSchema()
      df.show()
    })

  }
}
