package com.pixipanda.sparksql.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.Random


case class MakeModel(make: String, model: String)

case class T1(registration: String, make: String, model: String, engine_size: Double)

case class T2(make: String, model: String, engine_size: Double, sale_price: Double)

object CarSales {

  val makeModelSet: Seq[MakeModel] = Seq(
    MakeModel("FORD", "FIESTA")
    , MakeModel("NISSAN", "QASHQAI")
    , MakeModel("HYUNDAI", "I20")
    , MakeModel("SUZUKI", "SWIFT")
    , MakeModel("MERCEDED_BENZ", "E CLASS")
    , MakeModel("VAUCHALL", "CORSA")
    , MakeModel("FIAT", "500")
    , MakeModel("SKODA", "OCTAVIA")
    , MakeModel("KIA", "RIO")
  )

  def randomMakeModel(): MakeModel = {
    val makeModelIndex = if (Random.nextBoolean()) 0 else Random.nextInt(makeModelSet.size)
    Random.nextDouble()
    makeModelSet(makeModelIndex)
  }

  def randomEngineSize() = {
    //BigDecimal(s"1.${Random.nextInt(9)}")
    s"1.${Random.nextInt(9)}".toDouble
  }

  def randomRegistration(): String = s"${Random.alphanumeric.take(7).mkString("")}"

  def randomPrice() = 500 + Random.nextInt(5000)

  def randomT1(): T1 = {
    val makeModel = randomMakeModel()
    T1(randomRegistration(), makeModel.make, makeModel.model, randomEngineSize())
  }

  def randomT2(): T2 = {
    val makeModel = randomMakeModel()
    T2(makeModel.make, makeModel.model, randomEngineSize(), randomPrice())
  }


  def main(args: Array[String]) {

    val masterOfCluster = args(0)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load xml data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import sparkSession.implicits._

    val t1 = Seq.fill(10000)(randomT1()).toDS()

    val t2 = Seq.fill(100000)(randomT2()).toDS()


    t1.groupBy("make", "model").count().show(false)
    t2.groupBy("make", "model").count().show(false)

    //t1.show(false)
    //t2.show(false)

    /*t1.write
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .save("/tmp/carModel")

    t2.write
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .save("/tmp/carSales")*/

    /*val joinedDF = t1.join(t2, Seq("make", "model"))
      .filter(abs(t2("engine_size") - t1("engine_size")) <= BigDecimal("0.1"))
      .groupBy("registration")
      .agg(avg("sale_price").as("average_price"))

    joinedDF.show(false)*/

    scala.io.StdIn.readLine()

  }
}