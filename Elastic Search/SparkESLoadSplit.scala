package com.expedia.demos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkESLoadSplit {

  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      // .master("local[*]")
      .appName("SparkESLoadSplit")
      .getOrCreate()

    import spark.implicits._

    val propertyKeyChain = spark.read.parquet("s3n://ha-prod-omnidata-us-east-1/"+"market-intel/property-keychain-v2/property-keychains")

    val keyTypes = propertyKeyChain.select("key_type").distinct.collect.map{row => row.getString(0).toLowerCase}

    val keyTypeSpecificFrames =keyTypes.map{keytype => propertyKeyChain.filter($"key_type" === keytype)}


  }

}
