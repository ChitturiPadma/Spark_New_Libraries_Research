package com.expedia.demos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, levenshtein}
import org.apache.spark.sql.functions._

object SparkLevenshteinDistance {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkStringDistance")
      /*.config("fs.gs.project.id", "model-folio-818")
      .config("google.cloud.auth.service.account.enable", "true")
      .config("fs.gs.auth.service.account.json.keyfile", "<key_file>")*/
      .getOrCreate()

    import sparkSession.implicits._

    /*val haAbbFeatureDF = sparkSession.read.format("csv").option("header",true)
      .option("delimiter","\t")
      .load("/Users/pchitturi/Downloads/HA_ABBModel2FileWithIndoorMissingAllTranslated.csv")
      .withColumn("rental_heading_translated", when($"English".isNull or $"English" === "", $"rental_heading").otherwise($"English"))
      .withColumn("listing_title_translated_new", when($"listing_title_translated".isNull or $"listing_title_translated" === "", $"listing_title").otherwise($"listing_title_translated"))
      .withColumn("levenshtein_distance_new", levenshtein(col("rental_heading_translated"), col("listing_title_translated_new")))

    haAbbFeatureDF
      .coalesce(1)
      .write.format("csv")
      .option("header",true)
      .option("delimiter","\t")
      .save("/Users/pchitturi/Downloads/HA_ABB_Model2")*/

    val haAbbFeatureDF = sparkSession.read.format("csv").option("header",true)
      .option("delimiter","\t")
      .load("/Users/pchitturi/Downloads/HA_ABBFeaturesModel2TrainingFileFinal.csv")
      //.withColumn("rental_heading_translated", when($"English".isNull or $"English" === "", $"rental_heading").otherwise($"English"))
      //.withColumn("listing_title_translated_new", when($"listing_title_translated".isNull or $"listing_title_translated" === "", $"listing_title").otherwise($"listing_title_translated"))
      .withColumn("levenshtein_distance_new", levenshtein( lower(col("rental_heading_translated")), lower(col("listing_title_translated_new"))

    )
    )

    haAbbFeatureDF
      .coalesce(1)
      .write.format("csv")
      .option("header",true)
      .option("delimiter","\t")
      .save("/Users/pchitturi/Downloads/HA_ABB_Model2_TitleLower")

  }

}
