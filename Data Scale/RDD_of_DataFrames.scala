package com.expedia.demos

import org.apache.spark._
import org.apache.spark.sql.SparkSession

object RDD_of_DataFrames {

  def main(args:Array[String]): Unit = {



    val sess = SparkSession.builder().appName("SparkElasticSearchDemo")
      .master("local[*]")
      .appName("Read_CSVs_From_Paths")
      .getOrCreate()

    /*val rddCsvFiles = sess.sparkContext.makeRDD(Array("/Users/pchitturi/Documents/map-final.tsv"))

    // Creating Data Frames from a RDD of Paths.
    val csvFilesDf = rddCsvFiles.map{filePath => sess.read.option("delimiter", "\t").format("csv").load(filePath)}

    csvFilesDf.count*/


  }
}
