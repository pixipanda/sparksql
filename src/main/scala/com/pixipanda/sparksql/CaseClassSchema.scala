package com.pixipanda.sparksql

import java.util.Date

import org.apache.spark.sql.{Encoders, Row, SparkSession}

case class CreditcardTransaction(cc_num:String, transId:String, transTime:Date, category:String, merchant:String, amt:Double, merchLatitude:Double, merchLongitude:Double, isFraud:Int)

object TransactionEncoders {
  implicit def transactionEncoder: org.apache.spark.sql.Encoder[CreditcardTransaction] =
    org.apache.spark.sql.Encoders.kryo[CreditcardTransaction]
}


object CaseClassSchema {

  def main(args: Array[String]) {


    val masterOfCluster = args(0)
    val inputPath = args(1)


    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Programmatically Specifying the Schema")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import sparkSession.implicits._
    //import  TransactionEncoders._

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

    val transactionEncoder = Encoders.product[CreditcardTransaction]

    val transactiondf = sparkSession.sparkContext
      .textFile(inputPath)
      .map(transaction =>  {
      val fields = transaction.split(",")
      CreditcardTransaction(fields(0).trim, fields(1).trim,format.parse(fields(2).trim), fields(3).trim, fields(4), fields(5).trim.toDouble, fields(6).trim.toDouble, fields(7).trim.toDouble, fields(8).trim.toInt)
    }).toDS()


    transactiondf.printSchema()
    transactiondf.show()
  }
}