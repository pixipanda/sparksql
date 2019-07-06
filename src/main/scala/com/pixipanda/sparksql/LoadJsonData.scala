package com.pixipanda.sparksql

import org.apache.spark.sql.SparkSession


object LoadJsonData {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load json data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()



    val transactionDf = sparkSession.read.format("json").load(inputPath)

    transactionDf.printSchema()

    transactionDf.show(false)

  }
}