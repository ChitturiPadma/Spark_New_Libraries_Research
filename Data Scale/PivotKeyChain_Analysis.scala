package com.expedia.demos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PivotKeyChain_Analysis {

  def main(args:Array[String]): Unit ={

     val sparkSession = SparkSession.builder()
       .master("local[*]")
       .appName("PivotKeyChain_Analysis")
       .getOrCreate()

    import sparkSession.implicits._

    val filteredKeyChain = sparkSession
      .read.parquet("/Users/pchitturi/homeaway_projects/analysis/filtered_keychain_above_50m_distinct_pivoted")
    /*sparkSession
    .read.format("csv")
    .option("header", true)
    .load("/Users/pchitturi/homeaway_projects/analysis/filtered_keychain_above_50m_distinct_pivoted_csv")*/

    /*sparkSession
      .read.parquet("/Users/pchitturi/homeaway_projects/analysis/filtered_keychain_above_50m_distinct_pivoted")

    filteredKeyChain.coalesce(1).write.format("csv")
        .mode("overwrite")
        .option("header", true)
        .save("/Users/pchitturi/homeaway_projects/analysis/filtered_keychain_above_50m_distinct_pivoted_csv")*/

   /* println("Count: " + filteredKeyChain.count())

    filteredKeyChain.printSchema*/
    //filteredKeyChain.describe().show(100,false)

  /*  filteredKeyChain.agg(min("tot_cross_products_eles"), max("tot_cross_products_eles"))
      .show(100,false)

    filteredKeyChain.filter($"tot_cross_products_eles" === "8106712914041280000")
      .show(100,false)*/

   /* filteredKeyChain.write.format("csv")
      .mode("overwrite")
      .option("header", true)
      .save("/Users/pchitturi/homeaway_projects/analysis/filtered_keychain_above_50m_distinct_pivoted_csv_new")*/

    /*sparkSession.read.format("csv")
      .option("header", true)
      .load("/Users/pchitturi/homeaway_projects/analysis/filtered_keychain_above_50m_distinct_pivoted_csv_new")
      .agg(min("tot_cross_products_eles"), max("tot_cross_products_eles"))
      .show(100,false)*/

    println(filteredKeyChain
      .filter($"tot_cross_products_eles" > 10000000000L)
      .select("keychain_id").distinct.count)



  }

}
