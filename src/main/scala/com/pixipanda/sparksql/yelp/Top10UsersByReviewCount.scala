package com.pixipanda.sparksql.yelp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Top10UsersByReviewCount {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val userPath = args(1)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load json data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import sparkSession.implicits._

    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")


    val userDF = sparkSession.read.json(userPath)

    val reviewCountPerUserDF = userDF.select("user_id","name","review_count").orderBy('review_count.desc)

    reviewCountPerUserDF.show(10, false)

  }
}