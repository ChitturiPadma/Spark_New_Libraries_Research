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
                   .load("/Users/pchitturi/bigdata/data/green_tripdata_2015-07.csv")
                   .withColumn("point", point_col_func($"Pickup_longitude", $"Pickup_latitude"))

    //Loading neighborhood data
    val neighborhoods_Df = session.read.format("magellan").option("type", "geojson")
      .load("/Users/pchitturi/bigdata/data/neighborhoods.geojson")
      .select($"polygon", $"metadata" ( "neighborhood" ).as("neighborhood"))

    // Exploring various methods of Polygon
   /*
    val firstRow = neighborhoods_Df.select("polygon").first().getAs[Polygon](0)
    println("#Rings..."+firstRow.getNumRings)
    println("Ring at 0th index...."+firstRow.getRing(0))
    val rings = firstRow.getRings()
    println("Rings length..."+firstRow.getRings().length)
    println("Polygon type..."+firstRow.getType())
    val vertex = firstRow.getVertex(1)
    println("Vertex at 1st index...."+vertex)
    */

    /*
     Joining trips data with geometrical data to obtain the points lying within the polygon. This performs cartesian join
    */
    val joined_df = trips_Df.join(neighborhoods_Df).filter($"point" within $"polygon")
    val neighborhood_level_details = joined_df.groupBy("neighborhood").agg(count("*") as "RowCount").orderBy($"RowCount".desc)
    neighborhood_level_details.show(100, false)

    joined_df.explain(true)
    /*
    == Physical Plan ==
    CartesianProduct Within(point#55, polygon#81)
     */
    // Converting Polygon to RDD format and checking the available methods

    /*val polygon_Rdd = neighborhoods_Df.select("polygon").rdd.map{row => val polygon = row.getAs[Polygon](0)
      (polygon.getNumRings, polygon.getVertex(0))}

     polygon_Rdd.take(5).foreach(println)*/

    // Indexing the shapes to a given precision  which uses geohasher
    val indexedNeighborhoods_Df = neighborhoods_Df.withColumn("index", $"polygon" index 30)

    magellan.Utils.injectRules(session)

    val tripsWithinNeighborhood = trips_Df.join(indexedNeighborhoods_Df).filter($"point" within $"polygon")

    tripsWithinNeighborhood.explain(true)
    /*
    == Physical Plan ==
    *SortMergeJoin  [curve#194], [curve#196], Inner, ((relation#197 = Within) || Within(point#55, polygon#81))
    :- *Sort [curve#194 ASC NULLS FIRST], false, 0
     */

    //Intersects

    magellan.Utils.injectRules(session)
    val tripsIntersectingGeometry = trips_Df.join(indexedNeighborhoods_Df).filter($"point" intersects $"polygon")
    tripsIntersectingGeometry.show(10, false)

  }
}
