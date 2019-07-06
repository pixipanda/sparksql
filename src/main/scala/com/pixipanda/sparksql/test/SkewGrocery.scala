package com.pixipanda.sparksql.test

import org.apache.spark.sql.SparkSession


object SkewGrocery {

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

    val groceryPosDF = transactionDF.filter('category === "grocery_pos")


    val listOfgroceryPosDF =  for(i <- 0 to 1000 ) yield groceryPosDF


    val moreGroceryPosDF = listOfgroceryPosDF.reduce(_.union(_))

    val skewedDF = transactionDF.union(moreGroceryPosDF).coalesce(1)

    skewedDF.write.format("csv").options(parseOptions).save("/tmp/skewed")
  }
}