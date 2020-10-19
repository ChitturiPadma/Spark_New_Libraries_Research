package com.expedia.demos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkStringDistance {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkStringDistance")
      /*.config("fs.gs.project.id", "model-folio-818")
      .config("google.cloud.auth.service.account.enable", "true")
      .config("fs.gs.auth.service.account.json.keyfile", "<key_file>")*/
      .getOrCreate()

    import sparkSession.implicits._

    val haAbbFeatureDF = sparkSession.read.format("csv").option("header",true)
      .option("delimiter","\t")
      .load("/Users/pchitturi/Downloads/HA_ABB_SampleTest_FPs.csv")
      .withColumnRenamed("rental_heanding", "rental_heading")
        .withColumn("levenshtein_distance", levenshtein(col("rental_heading"), col("listing_title")))

    //haAbbFeatureDF.show(10,false)

    //haAbbFeatureDF.printSchema()

    haAbbFeatureDF
        .coalesce(1)
      .write.format("csv")
      .option("header",true)
      .option("delimiter","\t")
      .save("/Users/pchitturi/Downloads/HA_ABB_SampleTest_FPsNew.csv")



    /*println(haAbbFeatureDF.filter($"Label" ===1).count())
    println(haAbbFeatureDF.filter($"Label" ===0).count())*/

  /*  haAbbFeatureDF.select("rental_heading","listing_title","levenshtein_distance","Label")
      .show(100,false)*/
  }



}
