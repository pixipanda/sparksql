package com.pixipanda.sparksql.join

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object TipsBusiness {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val businessPath = args(1)
    val tipsPath = args(2)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load json data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import sparkSession.implicits._
    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")


    val businessDF = sparkSession.read.json(businessPath)
      .withColumn("attributes", to_json('attributes))
      .select('business_id, 'name, 'categories,  'address,  'city,  'state, 'is_open, 'review_count, 'stars)


    val tipsDF = sparkSession.read.json(tipsPath)


    val joinDF = businessDF.join(tipsDF, "business_id")


    joinDF.explain()

    joinDF.show(false)

  }
}