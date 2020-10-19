package com.expedia.demos

import java.io.PrintWriter

import com.expedia.demos.GephiVisualize_Sample.toGexf
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._


object GephiVisualize_Keychain {

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


  def main(args:Array[String]): Unit =
  {
    val spark = SparkSession.builder().master("local[*]").appName("main").getOrCreate()

    import spark.implicits._


    val vDf = spark.sparkContext.makeRDD{Array("6752627223108610824", "4194196513369558251", "-6562952033573248974",
      "-7336915225041817336", "7235098553630145404", "2840554435129310072", "-5910874445899563677", "-8775373489333249042",
      "2470064733702458243", "-5765630361300534425", "2730875857009599324", "8464809717020711858", "-7511926105465503849",
      "-2333621043877093639", "-4772400650648791777", "3412380870223161082", "7328833373543445119", "-2851733420040463661",
      "-4835542926963265364", "3516979237146957293")}.toDF("id")

    val vDfNew =  spark.sparkContext.makeRDD{Array("827946125894170429", "2450549532651029165", "2002892225619413970",
      "5230288330827049237", "-3290710792493658891")}.toDF("id")


    val verticesDf = spark.read.parquet("/Users/pchitturi/homeaway_projects/analysis/vertices")
      .withColumn("source_id", $"source_id".cast("string"))
      .join(vDfNew, col("source_id") === col("id"))
      .drop("id")
      .withColumn("source_id", $"source_id".cast("long"))





    val unionKeylinksTable = spark.read.parquet("/Users/pchitturi/homeaway_projects/analysis/edges")
      .withColumn("source_id", $"source_id".cast("string"))
      .withColumn("destination_id", $"destination_id".cast("string"))

      .join(vDfNew, col("source_id") === col("id") || col("destination_id") === col("id"))
      .drop("id")
      .withColumn("source_id", $"source_id".cast("long"))
      .withColumn("destination_id", $"destination_id".cast("long"))



    val graphVerticesRdd = verticesDf.rdd.map { row =>
      val idString: Long = row.getAs[Long]("source_id")
      (idString,
        (row.getAs[String]("source_key_type"),
          row.getAs[String]("source_key_value"),
          row.getAs[String]("keylink_type"),
          row.getAs[String]("match_type"),
          row.getAs[String]("match_date"),
          row.getAs[String]("match_confidence_score")
        ))
    }

    val edgesRdd = unionKeylinksTable.select("source_id", "destination_id",
      "source_key_type", "source_key_value",
      "destination_key_type", "destination_key_value", "keylink_type", "match_type",
      "match_date", "match_confidence_score").rdd.map(row => {
      Edge(
        row.getAs[Long]("source_id"),
        row.getAs[Long]("destination_id"),
        (row.getAs[String]("source_key_type"), row.getAs[String]("source_key_value"),
          row.getAs[String]("destination_key_type"), row.getAs[String]("destination_key_value"),
          row.getAs[String]("keylink_type"),
          row.getAs[String]("match_type"),
          row.getAs[String]("match_date"),
          row.getAs[String]("match_confidence_score")

        )
      )
    }).distinct()

    val nowhere = ("nowhere", "nowhere", "nowhere", "nowhere", "nowhere", "nowhere")

    val graph = Graph.apply(graphVerticesRdd, edgesRdd, nowhere)

   // val graphConnectedVertices = graph.connectedComponents().vertices

    val pw = new PrintWriter("/Users/pchitturi/homeaway_projects/analysis/graph_keychain_5nodes.gexf")
    pw.write(toGexf(graph))
    pw.close()
  }








}
