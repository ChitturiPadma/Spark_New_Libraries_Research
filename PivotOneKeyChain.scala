package com.expedia.demos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PivotOneKeyChain {

  def main(args:Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      // .master("local[*]")
      .appName("PivotOneKeyChain")
      .getOrCreate()

    sparkSession.conf.set("spark.sql.crossJoin.enabled", "true")
   // sparkSession.conf.set()

    import sparkSession.implicits._
    val omnidataBucket = "//ha-prod-omnidata-us-east-1/"
    val hivePropertyKeychainsTable = "tier1_dap_propertykeychain.property_keychains"

    val pivotAttributes = "homeaway_unit_uuid,homejunction_property_id,airbnb_property_id,expedia_property_id,bcom_unit_id," +
      "mls_sales_id,mls_listing_id,lps_native_property_id,standardized_property_address,standardized_building_address"

    val distinctPivotKeyTypes =  pivotAttributes.split(",")

    val keyTypesToSelect = "'" + pivotAttributes.toUpperCase.split(",").mkString("','") + "'"


    val propertyKeyChain = sparkSession.read
      .parquet("s3://ha-prod-omnidata-us-east-1/market-intel/property-keychain-v2/property-keychains-snapshot/pdateid=2020-05-07")
      .filter(col("key_type") =!= lit("MLS"))
      .where($"key_type".isin("HOMEAWAY_UNIT_UUID","HOMEJUNCTION_PROPERTY_ID", "AIRBNB_PROPERTY_ID", "EXPEDIA_PROPERTY_ID", "BCOM_UNIT_ID",
      "MLS_SALES_ID", "MLS_LISTING_ID", "LPS_NATIVE_PROPERTY_ID", "STANDARDIZED_PROPERTY_ADDRESS", "STANDARDIZED_BUILDING_ADDRESS"))


    /*val propertyKeyChain = sparkSession.sql(s"SELECT keychain_id, key_type, key_value " +
      s"FROM tier1_dap_propertykeychain.property_keychains WHERE key_type in ($keyTypesToSelect)")
      .filter(col("key_type") =!= lit("MLS"))*/


    val oneOfTheKeyChain =propertyKeyChain
      .filter($"keychain_id" === "57e3035070b271515aff721004b17b86e72a9713e21e9e49c753e539a6117d50")


    val haDf = oneOfTheKeyChain.filter($"key_type" === "HOMEAWAY_UNIT_UUID").select("key_value")
      .withColumnRenamed("key_value", "homeaway_unit_uuid")//.withColumRenamed("key_value", "homeaway_unit_uuid")

    val stBuildingAddress = oneOfTheKeyChain.filter($"key_type" === "STANDARDIZED_BUILDING_ADDRESS").select("key_value")
      .withColumnRenamed("key_value", "STANDARDIZED_BUILDING_ADDRESS".toLowerCase)//.withColumRenamed("key_value", "homeaway_unit_uuid")


    val stPropertyAddress = oneOfTheKeyChain.filter($"key_type" === "STANDARDIZED_PROPERTY_ADDRESS").select("key_value")
      .withColumnRenamed("key_value", "STANDARDIZED_PROPERTY_ADDRESS".toLowerCase)//.withColumRenamed("key_value", "homeaway_unit_uuid")


    val bcomDf = oneOfTheKeyChain.filter($"key_type" === "BCOM_UNIT_ID").select("key_value")
      .withColumnRenamed("key_value", "BCOM_UNIT_ID".toLowerCase)//.withColumRenamed("key_value", "homeaway_unit_uuid")

    val mlsSalesDf =  oneOfTheKeyChain.filter($"key_type" === "MLS_SALES_ID").select("key_value")
      .withColumnRenamed("key_value", "MLS_SALES_ID".toLowerCase)//.withColumRenamed("key_value", "homeaway_unit_uuid")


    val expediaDf =  oneOfTheKeyChain.filter($"key_type" === "EXPEDIA_PROPERTY_ID").select("key_value")
      .withColumnRenamed("key_value", "EXPEDIA_PROPERTY_ID".toLowerCase)//.withColumRenamed("key_value", "homeaway_unit_uuid")



    val lpsNativeDf =  oneOfTheKeyChain.filter($"key_type" === "LPS_NATIVE_PROPERTY_ID").select("key_value")
      .withColumnRenamed("key_value", "LPS_NATIVE_PROPERTY_ID".toLowerCase)


    val abbDf =  oneOfTheKeyChain.filter($"key_type" === "AIRBNB_PROPERTY_ID").select("key_value")
      .withColumnRenamed("key_value", "AIRBNB_PROPERTY_ID".toLowerCase)



    haDf.join(stBuildingAddress)

      .join(stPropertyAddress)
      .join(bcomDf)
      .join(mlsSalesDf)
      .join(expediaDf)
      .join(lpsNativeDf)
      .join(abbDf)
      .write.format("parquet")
      .mode("overwrite")
      .save("s3n://ha-prod-omnidata-us-east-1/Users/pachitturi/DPESRV-1245/onekeychain_crossproduct_result")

   /* haDf.crossJoin(stBuildingAddress)

      .crossJoin(stPropertyAddress)
      .crossJoin(bcomDf)
      .crossJoin(mlsSalesDf)
      .crossJoin(expediaDf)
      .crossJoin(lpsNativeDf)
      .crossJoin(abbDf)
      .write.format("parquet")
      .mode("overwrite")
      .save("s3n://ha-prod-omnidata-us-east-1/Users/pachitturi/DPESRV-1245/onekeychain_crossproduct_result")*/


  }

}
