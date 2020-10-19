package com.expedia.demos
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.json._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types.{LongType, StringType, StructField,StructType}
import org.apache.spark.sql.{Row}

import scala.util.Try
import scala.util.{Success, _}

import scala.collection.mutable.WrappedArray


object DataFrame_SortingWithinColumn {

  def rankAffinities(affinities_percentile:WrappedArray[String]) ={

    val affinities_percentile_tuple = affinities_percentile.toArray.map{field => val parts = field.split(":")
      (parts(0), parts(1))
    }
    affinities_percentile_tuple.groupBy(_._1).mapValues(_.map(_._2).max)
      .toArray
      .sortBy(_._2)(Ordering.String.reverse)
      .map(_._1)
  }

  def main(args:Array[String]): Unit =
  {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DataFrame_SortingWithinColumn")
      .getOrCreate()

    import spark.implicits._

    val udf_fun = udf(rankAffinities _)

    val affinitiesString = Array(("80159c42-cc09-4436-9ab6-417036615147","beach","1.0"), ("80159c42-cc09-4436-9ab6-417036615147","sunsets","1.0"),
      ("80159c42-cc09-4436-9ab6-417036615147","seafood","1.0"), ("80159c42-cc09-4436-9ab6-417036615147","excursions","1.0"),
      ("80159c42-cc09-4436-9ab6-417036615147","dolphins","0.9995999932289124"),
      ("80159c42-cc09-4436-9ab6-417036615147","port","0.9998999834060669"), ("80159c42-cc09-4436-9ab6-417036615147","sea","0.9990000128746033"),
      ("80159c42-cc09-4436-9ab6-417036615147","shopping","0.9998999834060669"),
      ("80159c42-cc09-4436-9ab6-417036615147","golf","0.9997000098228455"), ("80159c42-cc09-4436-9ab6-417036615147","dining","1.0"))

    val affinityDf = spark.sparkContext.makeRDD(affinitiesString).toDF("search_term_uuid", "affinity", "worldPercentile")
    val searchTermIdAffinities1 = affinityDf.select("search_term_uuid", "affinity","worldPercentile")
      .withColumn("affinity_percentile", concat($"affinity", lit(":"), $"worldPercentile") )
      .drop("affinity", "worldPercentile")

    val affinitiesPerSearchTerm_Df1 = searchTermIdAffinities1.groupBy("search_term_uuid")
      .agg(collect_set("affinity_percentile").as("affinities"))

    //searchTermIdAffinities1.show(10, false)
    //affinitiesPerSearchTerm_Df1.printSchema()

    val newlyMappedAffinities = affinitiesPerSearchTerm_Df1.withColumn("affinities", udf_fun($"affinities"))

     newlyMappedAffinities.show(1, false)
  }
}
