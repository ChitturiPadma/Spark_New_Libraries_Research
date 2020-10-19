package com.expedia.demos

import org.apache.spark._
import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object GraphFrames_Demo2 {

  def extractKeys(routes:Map[String, Int]) ={
    routes.filter{case(k,v) => v==1}.keys.toList.mkString(",")
  }


  def main(args:Array[String]): Unit = {

    val udf_extractKeys = udf(extractKeys _)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkGraph_Frames")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setCheckpointDir("./spark-checkpoint")

    val data = spark.read.option("header", "true").csv("/Users/pchitturi/Downloads/data-2.csv")

    val edgeDf = data.withColumn("src", concat($"source_key_type", lit(":"), $"source_key_value"))
      .withColumn("dst", concat($"destination_key_type", lit(":"), $"destination_key_value"))
    .select("src", "dst")

   // newDataDf.show(10, false)

   /* newDataDf.groupBy("src").agg(collect_set("dst"))
      .show(10, false)*/

   // data.show(10, false)

   val srcDf = edgeDf.select("src")
    val destDf = edgeDf.select("dst")
    val unionDf = srcDf.union(destDf)

    val vertexDf = unionDf.distinct().withColumnRenamed("src", "id")
    val allKeys = vertexDf.collect().map{row => row.getAs[String]("id")}

    val gFrame = GraphFrame(vertexDf, edgeDf)



    //unionDf.show(10, false)

   /* val shortestPathsDf =gFrame.shortestPaths.landmarks(allKeys).run()//.show(100, false)
      // .select($"distances",explode($"distances")).show(100, false)
      .withColumn("routes", udf_extractKeys($"distances"))
      .select("id", "routes")*/

   //println( shortestPathsDf.count)

    val connectedComponentsDf = gFrame.connectedComponents.run()
    connectedComponentsDf.show(100,false)
    //shortestPathsDf.show(2000, false)


  }

  }
