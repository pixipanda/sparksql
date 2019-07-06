package com.pixipanda.sparksql

import org.apache.spark.sql.SparkSession


object TotalAmtPerCredicardPerMerchant {


  def main(args: Array[String]) {


    val masterOfCluster = args(0)
    val inputPath = args(1)


    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load csv data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")

    val transactionDf = sparkSession.read.format("csv").options(parseOptions).load(inputPath)

    val totalAmtPerCreditcardPerMerchant = transactionDf.select("cc_num", "merchant", "amt")
     .groupBy("cc_num", "merchant").sum("amt")
     .show(false)
  }
}