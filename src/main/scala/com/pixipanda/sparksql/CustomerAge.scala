package com.pixipanda.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object CustomerAge {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val customerPath = args(1)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load csv data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import sparkSession.implicits._

    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")

    val customerDF = sparkSession.read.format("csv").options(parseOptions).load(customerPath)

    val customerAgeDF = customerDF.withColumn("age", (datediff(current_date(),to_date($"dob"))/365).cast(IntegerType))

    customerAgeDF.show(false)
  }

}