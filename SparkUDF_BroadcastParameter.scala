package com.expedia.demos

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.WrappedArray
import scala.collection.mutable._
import scala.collection._
import org.apache.spark.sql.functions._

object SparkUDF_BroadcastParameter {

  def rankAffinities(affinities_percentile:WrappedArray[String]) = {

      val mm = new HashMap[String, scala.collection.mutable.Set[String]] with MultiMap[String, String]
      val affinities_percentile_tuple = affinities_percentile.toArray.map{field => val parts = field.split(":")
        (parts(0), parts(1)) }
      affinities_percentile_tuple.foreach { case (key, value) => mm.addBinding(key, value) }
      val affinities_maxValues =  mm.mapValues(_.max)
      val sortedAffinities = affinities_maxValues.toArray.sortBy(_._2)(Ordering.String.reverse)//.map(_._1)(breakOut)
      sortedAffinities//.take(10).toArray
  }

  def main(args:Array[String]): Unit = {

    val sess = SparkSession.builder().appName("SparkUDF_BroadcastParameter")
      .master("local[*]")
      .appName("Pass broadcast variable as parameter to UDF ")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    import sess.implicits._

    val udf_affinities = udf(rankAffinities _)




    val affinityDf = sess.sparkContext
      .makeRDD(Array(("1", "beach", "1.0"), ("1", "beach", "0.9"), ("2", "coffee", "0.5"), ("2", "nature", "0.7")))
      .toDF("search_term_uuid", "affinity", "worldPercentile")

    val searchTermIdAffinities = affinityDf.select("search_term_uuid", "affinity","worldPercentile")
      .withColumn("affinity_percentile", concat($"affinity", lit(":"), $"worldPercentile") )
      .drop("affinity", "worldPercentile")

    val affinitiesPerSearchTerm_Df = searchTermIdAffinities.groupBy("search_term_uuid")
      .agg(collect_set("affinity_percentile").as("affinities"))

    //val multimap1 = new HashMap[String, scala.collection.mutable.Set[String]] with MultiMap[String, String]
    //val multimapBroadCasted = sess.sparkContext.broadcast(multimap1)


    rankAffinities _

//    val udf_rankAffinities = udf(rankAffinities _)

    val rankedAffinitiesPerSearchTerm_Df = affinitiesPerSearchTerm_Df.withColumn("affinities",
      udf_affinities($"affinities"))

 //   affinitiesPerSearchTerm_Df.show(10, false)


    rankedAffinitiesPerSearchTerm_Df.show(20, false)





  }

}
