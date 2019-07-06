package com.pixipanda.sparksql.schema

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._



object  StaticSchema {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)


    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Programmatically Specifying the Schema")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val transactionSchema = new StructType().add("cc_num", StringType, true)
      .add("trans_num", StringType, true)
      .add("trans_time", StringType, true)
      .add("category", StringType, true)
      .add("merchant", StringType, true)
      .add("amt", DoubleType, true)
      .add("merch_lat", DoubleType, true)
      .add("merch_long", DoubleType, true)
      .add("is_fraud", IntegerType, true)

    val transactionDf = sparkSession.read
      .format("csv")
      .schema(transactionSchema)
      .load(inputPath)

    transactionDf.show(false)
  }
}