package com.pixipanda.sparksql.join

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._


object  MerchantDistance {


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

    val transactionDF = sparkSession.read.format("csv").options(parseOptions).load(transactionPath)

    val customerDF = sparkSession.read.format("csv").options(parseOptions).load(customerPath)

    val joinedDF = transactionDF.join(customerDF, "cc_num")

    joinedDF.explain()

    /*val distanceUdf = udf(getDistance _)


    joinedDF.withColumn("distance", lit(round(distanceUdf($"lat", $"long", $"merch_lat", $"merch_long"), 2)))
      .select("cc_num", "first", "last", "gender", "job",  "trans_num", "trans_time", "category", "merchant", "amt", "distance", "dob", "is_fraud")
      .show(false)*/
  }
}