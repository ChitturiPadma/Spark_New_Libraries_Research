package com.expedia.demos



import org.apache.spark._
import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object GraphFramesMotif2 {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("GraphFramesMotif2")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setCheckpointDir("./spark-checkpoint")


    val vertices = spark.createDataFrame(List(
      ("HA1", "HOMEAWAY", 34),
      ("HA2", "HOMEAWAY", 36),
      ("ABB1", "AIRBNB", 30),
      ("ABB2", "AIRBNB", 29),
      ("BCOM1", "BOOKING", 32),
      ("HJ1", "HOMEJUNCTION", 36),
      ("HJ2", "HOMEJUNCTION", 60)
    )).toDF("id", "name", "value")

    val edges = spark.createDataFrame(List(
      ("HA1", "ABB1", "follow"),
      ("HA1", "HJ1", "follow"),
      //("c", "b", "follow"),
      ("HA1", "BCOM1", "follow"),
      ("HA1", "ABB2", "follow"),
      ("HA2", "ABB2", "follow"),
      ("HA2", "HJ2", "follow")
      )).toDF("src", "dst", "relationship")


    val keyChainVertices = spark.read.parquet("/Users/pchitturi/homeaway_projects/analysis/vertices")

    val keyChainEdges = spark.read.parquet("/Users/pchitturi/homeaway_projects/analysis/edges")
   /* val g = GraphFrame(vertices, edges)

    val motifs = g.find("(vertex1)-[]->(vertex2)")

    motifs.show(100, false)
*/

    println("vertices count: " + keyChainVertices.count)
    println("Edges count: " + keyChainEdges.count)



  }

}
