package com.pixipanda.sparksql.join

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object  TotalAmtPerJobTitle {

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

    import sparkSession.implicits._

    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")

    val transactionDF = sparkSession.read.format("csv").options(parseOptions).load(transactionPath)

    val customerDF = sparkSession.read.format("csv").options(parseOptions).load(customerPath)


    val transactionAmtDF  = transactionDF.select("cc_num", "amt")
    val customerJobTitleDF = customerDF.select("cc_num", "job")


    val joinedDF = transactionAmtDF.join(customerJobTitleDF, "cc_num")

    joinedDF.explain()


    val resultDF = joinedDF.groupBy("job").agg(sum('amt) as "totalsum").orderBy('totalSum desc)

    resultDF.show(50, false)

  }
}