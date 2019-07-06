package com.pixipanda.sparksql.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object TopNCustomerPerMerchant {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val transactionPath = args(1)


    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load csv data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import sparkSession.implicits._

    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")

    val transactionDf = sparkSession.read
      .format("csv")
      .options(parseOptions)
      .load(transactionPath)
      .select("category", "cc_num", "amt")

    val windowByCategoryAmtDesc = Window.partitionBy('category).orderBy('amt desc)

    val rankedDF = transactionDf.select('*, dense_rank().over(windowByCategoryAmtDesc) as 'rank)

    rankedDF.show(false)

    val top2CustomerDF = rankedDF.where('rank <= 2).show

  }
}