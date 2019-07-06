package com.pixipanda.sparksql.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object  CustomerMerchantDistance {


  def getDistance (lat1:Double, lon1:Double, lat2:Double, lon2:Double) = {
    val r : Int = 6371 //Earth radius
    val latDistance : Double = Math.toRadians(lat2 - lat1)
    val lonDistance : Double = Math.toRadians(lon2 - lon1)
    val a : Double = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2)
    val c : Double = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val distance : Double = r * c
    distance
  }


  def main(args: Array[String]) {


    val masterOfCluster = args(0)
    val transactionPath = args(1)
    val customerPath = args(2)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load csv data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import  sparkSession.implicits._

    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")

    val transactionDF = sparkSession.read
      .format("csv")
      .options(parseOptions)
      .load(transactionPath)
      .select("cc_num", "merchant", "merch_lat", "merch_long")

    val customerDF = sparkSession.read
      .format("csv")
      .options(parseOptions)
      .load(customerPath)
      .select("cc_num", "first", "last", "lat", "long")

    val joinedDF = transactionDF.join(customerDF, "cc_num")

    val distanceUdf = udf(getDistance _)

    val customerMerchantDistance = joinedDF.withColumn("distance", lit(round(distanceUdf($"lat", $"long", $"merch_lat", $"merch_long"), 2)))

    customerMerchantDistance.show(false)


    val distance = joinedDF
      .withColumn("a", pow(sin(radians($"merch_lat" - $"lat") / 2), 2) + cos(radians($"lat")) * cos(radians($"merch_lat")) * pow(sin(radians($"merch_long" - $"long") / 2), 2))
      .withColumn("distance", atan2(sqrt($"a"), sqrt(-$"a" + 1)) * 2 * 6371)


    distance.show(false)

  }
}