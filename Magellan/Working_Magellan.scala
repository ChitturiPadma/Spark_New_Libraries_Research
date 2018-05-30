package com.demos.geoanalytics

import magellan._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types._

object Working_Magellan {
  case class PolygonRecord(polygon: Polygon)

  def main(args:Array[String]): Unit =
  {

    //val conf = new SparkConf().setMaster("local[*]").setAppName("Sample_GeoSpark_App")
    val session = SparkSession.builder().appName("Sample_GeoSpark_App").master("local[*]").getOrCreate()
    val points = session.sparkContext.parallelize(Array((-1.0, -1.0), (-1.0, 1.0), (1.0, -1.0)))

    import session.implicits._
    val points_Df = points.toDF("x", "y").select(point($"x", $"y").as("point"))

    //points_Df.show(10, false)

    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))


    val polygons = session.sparkContext.parallelize(Seq(
      PolygonRecord(Polygon(Array(0), ring))
    )).toDF()


   /* val row = polygons.take(1).head

    val polygon_obj = row.getAs[Polygon](0)

    println("Ring.."+polygon_obj.getRing(0))

    polygon_obj.boundingBox
    println("Polugon object: "+polygon_obj)*/
   //    val ring_retrieved = polygon_obj.getRing(0)


    val points_withinPolygon_Df = points_Df.join(polygons).where($"point" within $"polygon")
    println("Matching points.."+ points_withinPolygon_Df.count )
    //points_withinPolygon_Df.show(5, false)


    //Intersects

    val intersect_Df = points_Df.join(polygons).where($"point" intersects $"polygon")
    intersect_Df.printSchema()

    val intersected_rdd = intersect_Df.rdd.map{row => (row.getAs[Point](0), row.getAs[Polygon](1))}
    val collected_points = intersected_rdd.collect
    collected_points.foreach(println)


    //println("Points intersecting the polygon..."+intersect_Df.count)
    //intersect_Df.show(5, false)


  }
}
