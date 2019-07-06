package com.pixipanda.sparksql.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object CumulativeSum {

  def main(args: Array[String]) {

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Load csv data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import sparkSession.implicits._

    val customersDF = sparkSession.sparkContext.parallelize(List(("Alice", "2016-05-01", 50.00),

      ("Alice", "2016-05-03", 45.00),

      ("Alice", "2016-05-04", 55.00),

      ("Bob", "2016-05-01", 25.00),

      ("Bob", "2016-05-04", 29.00),

      ("Bob", "2016-05-06", 27.00))).

      toDF("name", "date", "amountSpent")

    val cumulativeSumWindow = Window.partitionBy("name").orderBy("date").rowsBetween(Long.MinValue, 0)


    val cumulativeSum = customersDF.withColumn("cumSum", sum(customersDF("amountSpent")).over(cumulativeSumWindow))

    cumulativeSum.show(false)
  }
}