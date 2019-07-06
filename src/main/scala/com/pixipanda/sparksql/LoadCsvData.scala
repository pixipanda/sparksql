package com.pixipanda.sparksql

import org.apache.spark.sql.SparkSession


object LoadCsvData {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)


    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load csv data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val parseOptions = Map("header" -> "true", "inferSchema" -> "true", "multiLine" -> "True", "escape" -> """""")



    val transactionDf = sparkSession.read.format("csv").options(parseOptions).load(inputPath)

    transactionDf.printSchema()

    transactionDf.show(false)

    transactionDf.drop("is_fraud").write.format("csv").option("header", "true").save("/tmp/streamingtransaction")
  }
}