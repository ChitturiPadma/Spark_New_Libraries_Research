package com.expedia.demos

import java.io.PrintWriter

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GephiVisualize_Sample {

  def toGexf[VD, ED](g: Graph[VD, ED]): String = {
    val header =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<gexf xmlns="http://www.gexf.net/1.2draft" version="1.2">
        |  <meta>
        |    <description>A gephi graph in GEXF format</description>
        |  </meta>
        |    <graph mode="static" defaultedgetype="directed">
      """.stripMargin

    val vertices = "<nodes>\n" + g.vertices.map(
      v => s"""<node id=\"${v._1}\" label=\"${v._2}\"/>\n"""
    ).collect.mkString + "</nodes>\n"

    val edges = "<edges>\n" + g.edges.map(
      e => s"""<edge source=\"${e.srcId}\" target=\"${e.dstId}\" label=\"${e.attr}\"/>\n"""
    ).collect.mkString + "</edges>\n"

    val footer = "</graph>\n</gexf>"

    header + vertices + edges + footer
  }



  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder().master("local[*]").appName("main").getOrCreate()

    import spark.implicits._

    val vertices: RDD[(VertexId, String)] = spark.sparkContext.parallelize(
      Array((1L, "Anne"),
        (2L, "Bernie"),
        (3L, "Chris"),
        (4L, "Don"),
        (5L, "Edgar")))

    val edges: RDD[Edge[String]] = spark.sparkContext.parallelize(
      Array(Edge(1L, 2L, "likes"),
        Edge(2L, 3L, "trusts"),
        Edge(3L, 4L, "believes"),
        Edge(4L, 5L, "worships"),
        Edge(1L, 3L, "loves"),
        Edge(4L, 1L, "dislikes")))

    val graph: Graph[String, String] = Graph(vertices, edges)

    val pw = new PrintWriter("/Users/pchitturi/homeaway_projects/analysis/graph.gexf")
    pw.write(toGexf(graph))
    pw.close()
  }

}
