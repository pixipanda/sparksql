package com.pixipanda.sparksql

import org.apache.spark.sql.SparkSession


object LoadXmlData {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load xml data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val parseOptions = Map("rowTag" -> "transaction", "header" -> "true", "inferSchema" -> "true")

    val transactionDf = sparkSession.read.format("xml").options(parseOptions).load(inputPath)

    transactionDf.printSchema()

    transactionDf.show()

    scala.io.StdIn.readLine()


  }
}