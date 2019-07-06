package com.pixipanda.sparksql.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object YelpReviews {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load json data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import sparkSession.implicits._

    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")


    val reviewsDF = sparkSession.read.json(inputPath)


    /*val reviewsBusinessId = yelpReviews.filter('business_id.notEqual("4JNXUYY8wbaaDmk3BPzlWw"))

    val singleBussiness = reviewsBusinessId.withColumn("business_id", when('business_id.notEqual("4JNXUYY8wbaaDmk3BPzlWw"), "4JNXUYY8wbaaDmk3BPzlWw"))

    val skewedReview = yelpReviews.union(singleBussiness)

    val sampledSkewedReviews = skewedReview.sample(0.5)

    sampledSkewedReviews.coalesce(1).write.format("json").save("/tmp/skewedReviews")*/

    /*val groupByBusiness = sampledSkewedReviews.groupBy("business_id").count().orderBy('count.desc)
    groupByBusiness.show(false)*/

  }
}