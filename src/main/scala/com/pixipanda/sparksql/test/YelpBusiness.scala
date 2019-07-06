package com.pixipanda.sparksql.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object YelpBusiness {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load json data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import  sparkSession.implicits._


    val businessDF = sparkSession.read.json(inputPath)

    val cityCount = businessDF.select('city).distinct().count()

    println("cityCount: " + cityCount)


    businessDF.repartition(500, 'city).write.json("/tmp/cityrepartition")





    /*val b = yelpBusiness.withColumn("category", explode(
      when(col("categories").isNotNull, col("categories"))
        .otherwise(array(lit(null).cast("string")))
    )).select("business_id", "category")

    b.show(false)*/

    // Total Restaurants
    /*val totalRestaurants = yelpBusiness.withColumn("attributes", to_json('attributes))
      .withColumn("hours", to_json('hours))
      .filter(array_contains('categories, "Restaurants"))
      .count()

    println("Total Restaurants: " + totalRestaurants)*/



    /*val cat = yelpBusiness.withColumn("category", explode(when('categories.isNotNull, 'categories).otherwise(array(lit(null).cast("string")))))

    cat.groupBy('category, 'city)
      .agg(sum('review_count).alias("total_reviews"))
      .orderBy('total_reviews.desc)
      .select("city", "category", "total_reviews")
      .show(false)*/


    /*val cat = yelpBusiness.withColumn("category", explode(when('categories.isNotNull, 'categories).otherwise(array(lit(null).cast("string")))))
      .select('category)
      .distinct()
      .coalesce(1).write.format("csv")save("/tmp/distinctCategory")*/


    //yelpBusiness.groupBy("city").count().orderBy('count.desc).show(false)

    //yelpBusiness.filter('business_id === "4JNXUYY8wbaaDmk3BPzlWw").show(false)

  }
}