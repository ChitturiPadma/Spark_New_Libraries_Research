package com.demos.geoanalytics

import magellan._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.magellan._
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object UberTraffic_Analysis {

  def point_col(longitude:String, latitude:String) = Point(longitude.toDouble, latitude.toDouble)

  def main(args:Array[String]): Unit =
  {
    // Reading Trip data

    val session = SparkSession.builder().appName("TripData_Analysis").master("local[*]").getOrCreate()

    val point_col_func = udf(point_col _)
    import session.implicits._
    val trips_Df = session.read.format("csv")
                   .option("header", "true")
                   .load("/Users/pchitturi/bigdata/projects/green_tripdata_2015-07.csv")
                   .withColumn("point", point_col_func($"Pickup_longitude", $"Pickup_latitude"))

    //Loading neighborhood data

    val neighborhoods_Df = session.read.format("magellan").option("type", "geojson")
      .load("/Users/pchitturi/bigdata/data/neighborhoods.geojson")
      .select($"polygon", $"metadata" ( "neighborhood" ).as("neighborhood"))


    val joined_df = trips_Df.join(neighborhoods_Df).filter($"point" within $"polygon")

    println("joined count"+ joined_df.count)
    //joined_df.show(10, false)

    val neighborhood_level_det = joined_df.groupBy("neighborhood").agg(count("*") as "RowCount").orderBy($"RowCount".desc)

   neighborhood_level_det.show(100, false)
    // Converting Polygon to RDD format and checking the available methods

    /*val polygon_Rdd = neighborhoods_Df.select("polygon").rdd.map{row => val polygon = row.getAs[Polygon](0)
      (polygon.getNumRings, polygon.getVertex(0))}

     polygon_Rdd.take(5).foreach(println)*/





  }
}
