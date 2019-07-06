package com.pixipanda.sparksql.yelp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._



object  Top10UsersRatedBusinessCategory {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val usersPath = args(1)
    val reviewsPath = args(2)
    val businessPath = args(3)

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
      .select("user_id", "name", "business_id", "stars")
    userReviewJoinedDF.explain()


    val businessDF = sparkSession.read.json(businessPath)
    val businessCategoryDF = businessDF.withColumn("category", explode(
      when(col("categories").isNotNull, col("categories"))
        .otherwise(array(lit(null).cast("string")))
    )).select("business_id","category")


    val userReviewBusinessDF  = userReviewJoinedDF.join(businessCategoryDF, Seq("business_id"))
    userReviewBusinessDF.explain()

    //userReviewBusinessDF.show(false)

  }
}