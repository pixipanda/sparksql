package com.pixipanda.sparksql.udaf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._



class SumOfMaxUDAF(schema: StructType) extends UserDefinedAggregateFunction {

  /*This method describes the schema of input to the UDAF. Spark UDAFs can be defined to operate on any number of columns.*/
  def inputSchema: StructType = {
    this.schema
  }

  // Similar to inputSchema, this method describes the schema of UDAF buffer.
  // For SumOFMax UDAF, we need to maintain only one pieces of information. ie total sum for each group
  def bufferSchema: StructType = {
    StructType(Array(StructField("maxSum", DoubleType)))
  }

  // This describes the datatype of the output of UDAF.
  def dataType: DataType = {
    DoubleType
  }

  // This describes whether the UDAF we are implementing is deterministic or not.
  // Since, spark executes by splitting data, processing the chunks separately and combining them.
  // If the UDAF logic is such that the result is independent of the order in which data is
  // processed and combined then the UDAF is deterministic.
  def deterministic: Boolean = {
    true
  }

  // This method is used to initialize the buffer. This method is called once for each group
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
  }

  // This method takes a buffer and an input row and updates the
  // buffer. Any validations on input can be performed in this method
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0))
      buffer(0) = buffer.getDouble(0) + Array.range(0, input.length).map(input.getDouble).max
  }


  // This method takes two buffers and combines them to produce a
  // single buffer.
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
  }

  // This method will be called when all processing is complete and
  // there is only one buffer left. This will return the final value
  // of UDAF.
  def evaluate(buffer: Row): Double = {
    buffer.getDouble(0)
  }

}


object SumOfMax {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    import sparkSession.implicits._

    val DF = Seq(
      ("a", 5.0, 7.0, 11.0), // Max is 11.0
      ("a", 10.0, 4.0, 6.0), // Max is 10.0   Total sum for id a =  11.0 + 10.0 + 15.0 = 36.0
      ("a", 8.0, 15.0, 3.0), // Max is 15.0
      ("b", 6.8, 4.5, 3.3), //  Max is 6.8
      ("b", 9.2, 12.4, 6.7) //  Max is 12.4   Total sum for id b =  6.8 + 12.4 = 19.2
    ).toDF("ID","C1", "C2", "C3")


    val columns = Array("C1", "C2", "C3")
    val inputSchema = StructType(columns.map(StructField(_, DoubleType)))
    val sumOfMaxUDAF = new SumOfMaxUDAF(inputSchema)


    val sumOfMaxDF = DF.groupBy("ID").agg(sumOfMaxUDAF('C1, 'C2, 'C3).as("SumOfMax"))

    sumOfMaxDF.show()
  }

}

