package com.expedia.demos

import org.apache.spark._
import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Spark_GraphFrames_Demo {

  def extractKeys(routes:Map[String, Int]) ={
    routes.filter{case(k,v) => v >= 1}.keys.toList.mkString(",")
  }

  def main(args:Array[String]): Unit ={


    val udf_extractKeys = udf(extractKeys _)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkGraph_Frames")
      .getOrCreate()

    import spark.implicits._
   /* val edgeRDD = spark.sparkContext.makeRDD(Array(
      ("A", "B"),
      ("B", "C"),
      ("B", "D"),
      ("B", "E"),
      ("E", "F"),
      ("E", "G"),
      ("F", "G"),
      ("H", "I"),
      ("J", "I"),
      ("K", "L"),
      ("L", "M"),
      ("M", "N")
    ))*/

   val edgeRDD = spark.sparkContext.makeRDD(Array(
      (1L, 2L),
      (2L, 3L),
      (2L, 4L),
      (2L, 5L),
      (5L, 6L),
      (5L, 7L),
      (6L, 7L),
      (8L, 9L),
      (10L, 9L),
      (11L, 12L),
      (12L, 13L),
      (13L, 14L)
    ))

    val edgeRDDFinal = spark.sparkContext.makeRDD(Array(
      Edge(1L, 2L, "A"),
      Edge(2L, 3L, "B"),
      Edge(2L, 4L, "B"),
      Edge (2L, 5L, "B"),
      Edge (5L, 6L, "E"),
      Edge (5L, 7L,"E"),
      Edge (6L, 7L,"F"),
      Edge(8L, 9L,"H"),
      Edge (10L, 9L, "J"),
      Edge (11L, 12L, "K"),
      Edge(12L, 13L, "L"),
      Edge (13L, 14L, "M")
    ))





  /*  val edgeRDDOriginal = spark.sparkContext.makeRDD(Array(
      ("HA", "HJ"),
      ("HA", "ABB"),
      ("HA", "BCOM"),
      ("BCOM", "ABB"),
      ("HA", "MLS"),
      ("HJ", "MLS"),
      ("BCOM", "MLS")
    ))*/

    val edgeRDDOriginal = spark.sparkContext.makeRDD(Array(
      ("HA", "ABB"),
      ("HA", "BCOM"),
      ("HA", "MLS"),
      ("HJ", "MLS")

    ))


    val edgeDf = edgeRDD.toDF("src", "dst")

    val srcKeys = edgeRDD.keys
    val destKeys = edgeRDD.values


    val allKeysRdd:RDD[(VertexId, String)] = srcKeys.union(destKeys).distinct.map{id => (id, "HH")}

    val allKeys = allKeysRdd.collect()

   // val vertexDf = allKeysRdd.toDF("id")

    println("Printing Vertices")
    //vertexDf.show(100, false)

    val nowhere = ("nowhere", "nowhere")

   //val graph = Graph.apply(allKeysRdd, edgeRDDFinal, nowhere)

   // graph.connectedComponents().vertices.collect.foreach(println)

    val edgeDfOriginal = edgeRDDOriginal.toDF("src", "dst")

    val srcKeysOriginal = edgeRDDOriginal.keys
    val destKeysOriginal = edgeRDDOriginal.values

    val allKeysRddOriginal = srcKeysOriginal.union(destKeysOriginal).distinct

    val allKeysOriginal = allKeysRddOriginal.collect()



    val vertexDfOriginal = allKeysRddOriginal.toDF("id")



    val gFrame = GraphFrame(vertexDfOriginal, edgeDfOriginal)



    val vertexInDegrees = gFrame.inDegrees

    println("In Degrees")
    vertexInDegrees.show(100,false)

    val vertexOutDegrees = gFrame.outDegrees

   /* println("Out Degrees")
    vertexOutDegrees.show(100,false)*/

    //gFrame.vertices.show(100, false)



   //// gFrame.toGraphX.
   //println( gFrame.triplets.count)



    //gFrame.degrees.show(100, false)

    /*val shortestPathsDf =gFrame.shortestPaths.landmarks(allKeys).run()//.show(100, false)
     // .select($"distances",explode($"distances")).show(100, false)
        .withColumn("routes", udf_extractKeys($"distances"))
        .select("id", "routes")*/


    //shortestPathsDf.show(100, false)
    //gFrame.triplets
   // gFrame.triplets.show(20, false)


   /* spark.sparkContext.setCheckpointDir("./spark-checkpoint")
  val connectedComposDf = gFrame.connectedComponents.run()



  //  connectedComposDf.
    connectedComposDf.show(100, false)
*/
   // gFrame.connectedComponents.

  // println(connectedComposDf.count)

    //gFrame

   // gFrame._vertices.show(100, false)

   // val shortestPathsDf = gFrame.shortestPaths.run()

   // shortestPathsDf.show(100, false)

    //connectedComposDf.show(20, false)


  }

}
