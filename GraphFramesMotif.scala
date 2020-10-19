package com.expedia.demos


import org.apache.spark._
import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object GraphFramesMotif {

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("GraphFramesMotif")
      .getOrCreate()

import spark.implicits._

    spark.sparkContext.setCheckpointDir("./spark-checkpoint")

    val vertices = spark.createDataFrame(List(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
      ("d", "David", 29),
      ("e", "Esther", 32),
      ("f", "Fanny", 36),
      ("g", "Gabby", 60)
    )).toDF("id", "name", "age")

    val edges = spark.createDataFrame(List(
      ("a", "b", "follow"),
      ("b", "c", "follow"),
      //("c", "b", "follow"),
      ("f", "c", "follow"),
      ("e", "f", "follow"),
      ("e", "d", "follow"),
      ("d", "a", "follow"),
      ("a", "e", "follow"))).toDF("src", "dst", "relationship")

    val g = GraphFrame(vertices, edges)

    val motifs = g.find("(a)-[]->(b); (b)-[]->(c)")

    motifs.show(100, false)

    //g.vertices.show(20,false)
 // g.edges.show(20, false)

   /* val vertexInDegrees = g.inDegrees

    println("In Degrees")
    vertexInDegrees.show(100,false)

    val vertexOutDegrees = g.outDegrees

    println("Out Degrees")
    vertexOutDegrees.show(100,false)

    println("Connected Components")*/
   /* val result = g.toGraphX.connectedComponents



    val connectedVerticesDf = result.vertices.map(row => {
        (row._1, row._2)
      }).toDF("vertex_id", "component_id")

    connectedVerticesDf.show(200,false)*/

  /*  val connectedComponentsDf = g.connectedComponents.run()
    connectedComponentsDf.show(100,false)*/



   // g.shortestPaths.run().show(200,false)

    /*g.triplets

    val shortestPathResult =  g.shortestPaths.landmarks(allKeys)
      .run()
    shortestPathResult.select("id", "distances").show(100,false)*/




  }

}
