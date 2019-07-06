package com.pixipanda.sparksql.yelp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Top10UsersRatedBusiness {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val usersPath = args(1)
    val reviewsPath = args(2)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load json data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import sparkSession.implicits._

    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")

    val usersDF = sparkSession.read.json(usersPath)
    val top10UsersDF = usersDF.select("user_id","name","review_count").orderBy('review_count.desc).limit(10)


    val reviewsDF = sparkSession.read.json(reviewsPath)
    val reviewStarsDF = reviewsDF.select("user_id","business_id","stars")


    val userReviewJoinedDF  = reviewsDF.join(top10UsersDF, Seq("user_id"))
    userReviewJoinedDF.explain()


    val top10UsersRatedBusinessDF = userReviewJoinedDF
      .select("user_id", "name", "business_id", "stars")
      .orderBy('stars.desc)

    //top10UsersRatedBusinessDF.show(false)
  }
}