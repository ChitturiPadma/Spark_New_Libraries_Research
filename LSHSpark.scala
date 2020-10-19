package com.expedia.demos.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import breeze.linalg.{DenseVector, SparseVector}
import breeze.linalg.functions._
//import com.github.fommil.netlib.BLAS
import org.apache.spark.mllib.linalg.BLAS
import org.apache.spark.mllib.linalg.Vectors._


object LSHSpark {

  def main(args:Array[String]): Unit =
  {



    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("LSHSpark")
      /*.config("fs.gs.project.id", "model-folio-818")
      .config("google.cloud.auth.service.account.enable", "true")
      .config("fs.gs.auth.service.account.json.keyfile", "<key_file>")*/
      .getOrCreate()



    import sparkSession.implicits._

    //BLAS.dot()



    //val x =BLAS.dot(c,d)


    val haHaFeatureDataset =sparkSession.read.format("csv").option("header",true)
      .option("delimiter","\t")
      .load("/Users/pchitturi/Downloads/latest_with_amenities2.csv")
        .select("allfeat_source_attribute_area", "allfeat_matched_attribute_area",
    "allfeat_indoor_calc","allfeat_indoor_match_calc","allfeat_bedroom_match_calc","allfeat_bathroom_match_calc","allfeat_kitchen_match_calc",
    "allfeat_yard_match_calc","allfeat_landscape_match_calc","allfeat_livingroom_match_calc","allfeat_others_in_match_calc","allfeat_others_out_match_calc",
    "allfeat_otherrooms_match_calc","allfeat_exterior_match_calc","allfeat_diningroom_match_calc","allfeat_swimming_match_calc","allfeat_balcony_match_calc",
    "allfeat_bathroom_diff","allfeat_bedroom_diff","allfeat_image_match_percent","allfeat_indoor_match_percent","allfeat_latlong_distance_unit",
          "allfeat_matched_property_title","allfeat_sleeps_diff","allfeat_source_property_title"
        )

    /*val haHaFeatureColumns = haHaFeatureDataset.columns

    val principalComponentsColumn =haHaFeatureColumns.map{colname => col(colname)}.toSeq

    val nearNeighboursNumber = 4
    val hashesNumber = 3*/

    haHaFeatureDataset.printSchema()





    /*println("Count of Rows: " + haHaFeatureDataset.count())
    println("Columns in the FeatureDatset : " + haHaFeatureDataset.columns.mkString(","))*/
  }
}
