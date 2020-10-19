package com.expedia.demos


import org.apache.spark._
import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Spark_GraphFrames_Final {

  def extractPaths(routes:Map[Long, Int]) ={
    routes.filter{case(k,v) => v >= 1}.keys.toList.mkString(",")
  }
  def main(args:Array[String]): Unit =
  {


    val udf_extractPaths = udf(extractPaths _)

    val spark = SparkSession.builder()
      .appName("SparkGraph_Frames_Final")
      .getOrCreate()

    import spark.implicits._

   val addressKeyLinks = spark.read.parquet("s3n://ha-stage-omnidata-us-east-1/market-intel/property-keychain-v2/property-keychains-graphx/random-address-keylinks")

    val imageKeyLinks = spark.read.parquet("s3n://ha-stage-omnidata-us-east-1/market-intel/property-keychain-v2/property-keychains-graphx/random-image-keylinks")

    val allKeyLinksDf = addressKeyLinks.union(imageKeyLinks)

    val edgeDf = allKeyLinksDf.select("source_id", "destination_id")
      .withColumnRenamed("source_id", "src")
      .withColumnRenamed("destination_id", "dst")

    val sourceDf  = edgeDf.select("src")
    val destDf = edgeDf.select("dst")

    val vertexDf = sourceDf.union(destDf).withColumnRenamed("src", "id")
    val allKeys = vertexDf.collect().map{row => row.getAs[Long]("id")}
    val gFrame = GraphFrame(vertexDf, edgeDf)

    val shortestPathsDf =gFrame.shortestPaths.landmarks(allKeys).run()//.show(100, false)
      // .select($"distances",explode($"distances")).show(100, false)
      .withColumn("routes", udf_extractPaths($"distances"))
      .select("id", "routes")

    shortestPathsDf.write.mode(SaveMode.Overwrite).parquet("s3n://ha-stage-omnidata-us-east-1/pachitturi/shotestPaths_Data")




  }

}
