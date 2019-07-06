package com.pixipanda.sparksql.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, IntegerType, StructType}

object LoadSampleXmlData {

  def main(args: Array[String]) {


    val masterOfCluster = args(0)
    val inputPath = args(1)
    val jsonOutput = args(2)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load xml data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import sparkSession.implicits._

    val parseOptions = Map("rowTag" -> "note", "header" -> "true", "inferSchema" -> "true")

    val df = sparkSession.read.format("xml").options(parseOptions).load(inputPath)

    val schema = new StructType().add("body", StringType)
      .add("from", StringType)
      .add("heading", StringType)
      .add("to", StringType)
      .add("triggertime", LongType)

    val jsondf = df.toJSON
      .withColumn("jcolumn", from_json('value, schema))
      .select("jcolumn.*")


    jsondf.show(false)

    //df.write.format("json").options(parseOptions).save(jsonOutput)

   /* df.printSchema()

    df.show()*/

  }
}