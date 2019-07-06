package com.pixipanda.sparksql.schema

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object ProgrammaticSchema {


  val sparkSQLDataType = Map("long" -> LongType, "string" -> StringType, "timestamp" -> TimestampType, "int" -> IntegerType, "double" -> DoubleType)

  def getSchema(fileName:String) = {

    val fields = scala.io
      .Source
      .fromFile(fileName)
      .getLines().toArray
      .map(element => {
        val field = element.split(" ")
        val fieldName = field(0)
        val fieldType = field(0).toLowerCase
        val sparkType = sparkSQLDataType.getOrElse(fieldType, StringType)

        StructField(field(0), sparkType, true)
      })
    StructType(fields)
  }



  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)
    val schemaFile = args(2)


    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Programmatically Specifying the Schema")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val transactionSchema = getSchema(schemaFile)

    val transactionDf = sparkSession.read
      .format("csv")
      .schema(transactionSchema)
      .load(inputPath)


    transactionDf.show(false)
  }
}