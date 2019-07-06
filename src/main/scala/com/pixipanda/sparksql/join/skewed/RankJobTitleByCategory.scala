package com.pixipanda.sparksql.join.skewed

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object  RankJobTitleByCategory {

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


    import  sparkSession.implicits._
    
    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")

    val transactionDF = sparkSession.read.format("csv").options(parseOptions).load(transactionPath)

    transactionDF.groupBy("cc_num").count().orderBy('count desc).show(100, false)

    val customerDF = sparkSession.read.format("csv").options(parseOptions).load(customerPath)


    val joinedDF = transactionDF.join(customerDF, "cc_num")//.select("category", "job", "amt")

    joinedDF.explain()

    val groupedDF = joinedDF.groupBy("category", "job").agg(sum("amt").as("totalAmt"))//.orderBy('category, 'totalAmt desc)


    groupedDF.explain()

   // groupedDF.coalesce(1).write.format("csv").options(parseOptions).save("/tmp/totalAmtPerCategoryPerJob")

    val windowByJob = Window.partitionBy("job").orderBy('totalAmt desc)

    val rankedDF = groupedDF.select('*, rank.over(windowByJob) as 'rank)

    //rankedDF.coalesce(1).write.format("csv").options(parseOptions).save("/tmp/rankByCategoryByJob")

    /*val windowByJobTitle =  Window.partitionBy('category, 'job).orderBy('amt desc)

    val rankedDF = joinedDF.select('*, rank.over(windowByJobTitle) as 'rank)

    rankedDF.show(false)*/


  }


}