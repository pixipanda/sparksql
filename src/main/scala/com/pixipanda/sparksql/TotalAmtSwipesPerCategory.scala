package com.pixipanda.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object  TotalAmtSwipesPerCategory {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)


    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load csv data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    import  sparkSession.implicits._

    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")

    val transactionDF = sparkSession.read.format("csv").options(parseOptions).load(inputPath)

    val groupedDF = transactionDF.groupBy('category)

    groupedDF.agg(sum("amt") as "totalsum",count("cc_num") as "totalcount")
      .orderBy('totalsum desc)
      .show(false)
  }
}