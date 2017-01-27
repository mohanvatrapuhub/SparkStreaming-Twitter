package com.vatrapu

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.log4j.Level
import org.joda.time.DateTime
import twitter4j.auth.Authorization
import twitter4j.Status
import twitter4j.auth.AuthorizationFactory
import twitter4j.conf.ConfigurationBuilder

/**
  * Created by MohanChaitanyaReddyVatrapu on 1/25/17.
  */
object TwitterStream {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("Tweet")
      .config("spark.cleaner.ttl", "3600")
      .getOrCreate()


    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))


    import spark.implicits._  //Used to convert Scala objects into Dataframes


    val consumerKey = " _ "
    val consumerSecret = " _ "
    val accessToken = " _ "
    val accessTokenSecret = " _ "


    val conf = new ConfigurationBuilder()
    conf.setOAuthAccessToken(accessToken)
    conf.setOAuthAccessTokenSecret(accessTokenSecret)
    conf.setOAuthConsumerKey(consumerKey)
    conf.setOAuthConsumerSecret(consumerSecret)

    val auth = Option(AuthorizationFactory.getInstance(conf.build()))

    //Setting up an array of keywords which are found in a tweet which you want to extract
    val keyWord = Array("Trump")

    val tStream = TwitterUtils.createStream(ssc, auth, keyWord)


    //Getting the Hashtags within the tweets and counts them, which contains the particular keyword
    //and saving as text files in local machine
    val tags = tStream.flatMap { status =>
      status.getHashtagEntities.map(_.getText)
    }
    tags.countByValue()
       .foreachRDD { rdd =>
        val now = org.joda.time.DateTime.now()
        rdd
          .sortBy(_._1)
          .map(x => (x, now))
          .saveAsTextFile(s"/Users/MohanChaitanyaReddyVatrapu/documents/twitter/$now")
      }



    //Extracting Hashtags "#" from tweets and finding the count of the Hashtags from set of tweets
    val statuses = tStream.flatMap(status => status.getText.split(" ")).filter(a => a.startsWith("#"))
                                .map(x => (x, 1))
                                      .reduceByKey((x, y) => x + y).transform(_.sortByKey())
    statuses.print()




    ssc.start()

    ssc.awaitTermination()
  }

}
