package com.expedia.demos.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.graphframes.GraphFrame

object GraphFrames_GraphXAlgos {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("GraphFrames_GraphXAlgos")
      .getOrCreate()

    import sparkSession.implicits._

    val edgeRdd = sparkSession.sparkContext.makeRDD{
      Array(
        ("HA1", "ABB1"),
        ("HA1", "HJ1"),
        ("HA1", "BCOM1"),
        ("HA1", "ABB2"),
        ("HA2", "ABB2"),
        ("HA2", "HJ2")
      )
    }

    val edgeDf = edgeRdd.toDF("src", "dst")

    val srcKeys = edgeRdd.keys
    val destKeys = edgeRdd.values

    val vertexDf  = srcKeys.union(destKeys).distinct().toDF("id")


    /*val g = GraphFrame(vertexDf, edgeDf)

    println("InDegrees")
    val inDegrees  = g.inDegrees
    inDegrees.show(100,false)*/


    val verticesRdd2:RDD[(VertexId, String)] = sparkSession.sparkContext.makeRDD{
      Array((3L, ("nothing")), (7L,("nothing")), (5L,("nothing")),   (2L, ("nothing")))
    }

    val edgesRdd2 = sparkSession.sparkContext.makeRDD{
      Array(
        Edge(3L, 7L, "nothing"), Edge(5L, 3L,"nothing"), Edge(2L, 5L, "nothing"), Edge(5L, 7L, "nothing")
      )
    }

    val graph2 = Graph.apply(verticesRdd2, edgesRdd2)
/*
    val inDegrees2 = graph2.inDegrees

    println("InDegrees")
    inDegrees2.collect().foreach(println)*/

    // Edge triplet

    graph2.triplets.collect().foreach(println)



  }

}
