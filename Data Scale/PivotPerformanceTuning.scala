package com.expedia.demos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PivotPerformanceTuning {

  def main(args:Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      // .master("local[*]")
      .appName("PivotPerformanceTuning")
      .getOrCreate()


    val omnidataBucket = "//ha-prod-omnidata-us-east-1/"
    val hivePropertyKeychainsTable = "tier1_dap_propertykeychain.property_keychains"

    val KEY_TYPE = "key_type"
    val KEY_VALUE = "key_value"
    val KEYCHAIN_ID = "keychain_id"

    val pivotAttributes =  "homeaway_unit_uuid,homejunction_property_id,airbnb_property_id,expedia_property_id,bcom_property_id,mls_sales_id,mls_listing_id,lps_native_property_id"

    val distinctPivotKeyTypes =  pivotAttributes.split(",")

    val keyTypesToSelect = "'" + pivotAttributes.toUpperCase.split(",").mkString("','") + "'"

    import sparkSession.implicits._

    val propertyKeyChain = sparkSession.sql(s"SELECT keychain_id, key_type, key_value " +
      s"FROM $hivePropertyKeychainsTable WHERE key_type in ($keyTypesToSelect)")
      .filter(col(KEY_TYPE) =!= lit("MLS"))  // todo: remove this key type

    val pivottedPropertyKeyChain =  propertyKeyChain
      .repartition(300)
      .groupBy(KEYCHAIN_ID)
      .pivot(KEY_TYPE)
      .agg(collect_list(KEY_VALUE))
      .select(KEYCHAIN_ID, distinctPivotKeyTypes: _*)


    pivottedPropertyKeyChain
      .withColumn("homeaway_unit_uuid", explode_outer(col("homeaway_unit_uuid")  ))

      //.withColumn("homejunction_property_id", explode_outer(col("homejunction_property_id")  ) )


    //  .withColumn("airbnb_property_id", explode_outer(col("airbnb_property_id")  ) )



      .withColumn("expedia_property_id", explode_outer(col("expedia_property_id")  ) )
      .withColumn("bcom_property_id", explode_outer(col("bcom_property_id")  ) )
      .withColumn("mls_sales_id", explode_outer(col("mls_sales_id")  ) )

      .withColumn("mls_listing_id", explode_outer(col("mls_listing_id")  ) )

      .withColumn("lps_native_property_id", explode_outer(col("lps_native_property_id")  ) )
      .write.format("parquet")
      .mode("overwrite")
      .save("s3n://ha-prod-omnidata-us-east-1/Users/pachitturi/DPESRV-1068/pivot-without-hj-abb")




  }

}
