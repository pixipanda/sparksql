package com.pixipanda.sparksql.yelp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object AvgStarsPerCategoryPerCity {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val businessPath = args(1)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load json data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import sparkSession.implicits._

    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")


    val businessDF = sparkSession.read.json(businessPath)


    val businessCategoryDF = businessDF.withColumn("category", explode(
      when(col("categories").isNotNull, col("categories"))
        .otherwise(array(lit(null).cast("string")))
    ))


    val avgStar = businessCategoryDF.groupBy("category", "city")
      .agg(avg('stars) as "avg_stars")
      .orderBy('avg_stars.desc)
      .select("city", "category", "avg_stars")


    avgStar.show(false)
  }
}