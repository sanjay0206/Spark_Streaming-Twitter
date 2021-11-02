package com.spark.twitter.config

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import java.io.FileInputStream
import java.util.Properties

object TwitterConfig {
  def setCredentials(): OAuthAuthorization = {
    val properties = new Properties()
    val config = new FileInputStream("config.properties")
    properties.load(config)

    val consumerKey = properties.getProperty("consumerKey")
    val consumerSecret = properties.getProperty("consumerSecret")
    val accessToken = properties.getProperty("accessToken")
    val accessTokenSecret = properties.getProperty("accessTokenSecret")

    val configurationBuilder = new ConfigurationBuilder()
    configurationBuilder
      .setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

    val auth = new OAuthAuthorization(configurationBuilder.build())
    auth
  }
}
