package com.pixipanda.sparksql.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load csv data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import sparkSession.implicits._

    val parseOptions = Map("header" -> "true", "inferSchema" -> "true")

    val transactionDf = sparkSession.read.format("csv").options(parseOptions).load(inputPath)

    //transactionDf.groupBy("cc_num").count().orderBy('count desc).show(50, false)

    //println(transactionDf.select("merchant").distinct().count())

    //println(transactionDf.select("category").distinct().count())

    //transactionDf.groupBy("merchant").count().orderBy('count desc).coalesce(1).write.format("csv").options(parseOptions).save("/tmp/swipespermerchant")
    //val groupedData = transactionDf.groupBy("category").agg(sum("amt") as "totalsum",count("cc_num") as "totalcount").orderBy('totalsum desc).show(false)
    val categoryDF = transactionDf.repartition('category)

    categoryDF.write.format("csv").save("/tmp/category")

    //scala.io.StdIn.readLine()

    sparkSession.stop()

  }
}