package com.pixipanda.sparksql.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object AvgAmtPerCategory {

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

    val transactionDf = sparkSession.read.format("csv").options(parseOptions).load(transactionPath)

    val windowByCategory = Window.partitionBy('category)

    val avgAmtPerCategory = transactionDf.withColumn("avg", avg("amt") over windowByCategory)
                               .select("cc_num", "category", "merchant", "amt", "avg")

    avgAmtPerCategory.show(100, false)


  }
}