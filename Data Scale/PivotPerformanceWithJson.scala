package com.expedia.demos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection._

import org.json4s.DefaultFormats
import org.json4s.native.Json

object PivotPerformanceWithJson {

  def main(args:Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      // .master("local[*]")
      .appName("PivotPerformanceWithJson")
      .getOrCreate()


    val omnidataBucket = "//ha-prod-omnidata-us-east-1/"
    val hivePropertyKeychainsTable = "tier1_dap_propertykeychain.property_keychains"

    val KEY_TYPE = "key_type"
    val KEY_VALUE = "key_value"
    val KEYCHAIN_ID = "keychain_id"

    val pivotAttributes =  "homeaway_unit_uuid,homejunction_property_id,airbnb_property_id,expedia_property_id,bcom_property_id,mls_sales_id,mls_listing_id,lps_native_property_id"

    val distinctPivotKeyTypes =  pivotAttributes.split(",")

    val distinctPivotKeyTypesUpperCase = distinctPivotKeyTypes.map{keyType => keyType.toUpperCase}.toList

    //val keyTypesToSelect = "'" + pivotAttributes.toUpperCase.split(",").mkString("','") + "'"

    val keyTypesToSelect = pivotAttributes.toUpperCase.split(",")

    import sparkSession.implicits._

    val propertyKeyChain = sparkSession.read
      .parquet("s3n://ha-prod-omnidata-us-east-1/market-intel/property-keychain-v2/property-keychains-snapshot/pdateid=2019-12-19")
      .select("keychain_id", "key_type", "key_value" )
      .filter($"key_type".isin(keyTypesToSelect: _*))
      .filter(col(KEY_TYPE) =!= lit("MLS"))

    /*
    val propertyKeyChain = sparkSession.sql(s"SELECT keychain_id, key_type, key_value " +
      s"FROM $hivePropertyKeychainsTable WHERE key_type in ($keyTypesToSelect)")
      .filter(col(KEY_TYPE) =!= lit("MLS"))  // todo: remove this key type
*/


      .withColumn("key_type_key_value", trim(concat(col("key_type"), lit(":"), col("key_value"))) )
      .select("keychain_id","key_type_key_value" )

      .rdd.map{row =>

      val keychain_id = row.getAs[String]("keychain_id")

      val key_type_key_value = row.getAs[String]("key_type_key_value")

      (keychain_id, key_type_key_value)
    }
      .repartition(800)

     /* .groupByKey.flatMap{

      case(keychain_id: String, key_type_key_valueIter:Iterable[String]) =>

        val key_type_key_valueList =key_type_key_valueIter.toList
        val key_type_key_valuesSeparateLists = key_type_key_valueList.groupBy(ele => ele.split(":")(0)).map{t => t._2}.toList

        val keyTypes = key_type_key_valueList.map{ele => ele.split(":")(0)}

        val extraKeyTypes = distinctPivotKeyTypesUpperCase.diff(keyTypes)
        val extraKeyTypeValuePairs:List[(String, String)] = extraKeyTypes.map{keytype => (keytype,null)}

        val key_type_key_valuesCrossJoin = key_type_key_valuesSeparateLists.reduce((X,Y) => X.flatMap{ele1 => Y.map{ele2 => ele1 + ";" + ele2} })

        val requiredKeyTypeValuesPairs = key_type_key_valuesCrossJoin.map{str => str.split(";").toList}.map{ll => ll.map{ele => val parts = ele.split(":")
          (parts(0), parts(1)) } ++ extraKeyTypeValuePairs }

        val requiredKeyTypeValuesPairswithKeychainId = requiredKeyTypeValuesPairs.map{ll => ll.+:("keychain_id",keychain_id)}

        val requiredKeyTypeValuesPairswithKeychainIdMap = requiredKeyTypeValuesPairswithKeychainId.map{ll => Map(ll.toSeq:_*)}

        requiredKeyTypeValuesPairswithKeychainIdMap.map{keyTypeValueMap => Json(DefaultFormats).write(keyTypeValueMap) }

    }*/


      .reduceByKey{(key_type_key_value1, key_type_key_value2) => key_type_key_value1 + "," + key_type_key_value2}
      .map{case(keychain_id:String, key_type_key_value_reduced:String) =>

        val key_type_key_valueList =key_type_key_value_reduced.split(",")

        (keychain_id,key_type_key_valueList.toList )
      }
      .map{
        case(keychain_id:String, key_type_key_valueList:List[String]) =>

          val key_type_key_valuesSeparateLists = key_type_key_valueList.groupBy(ele => ele.split(":")(0)).map{t => t._2}(breakOut).toList

          val key_type_key_valuesCrossJoin = key_type_key_valuesSeparateLists.reduce((X,Y) => X.flatMap{ele1 => Y.map{ele2 => ele1 + ";" + ele2} })

          (keychain_id, key_type_key_valuesCrossJoin)

      }



   /*val  propertyKeyChainDs = sparkSession.read.json(propertyKeyChain)

    propertyKeyChainDs
        .repartition(800)
      .write.format("parquet")
      .mode("overwrite")
      .save("s3n://ha-prod-omnidata-us-east-1/Users/pachitturi/DPESRV-1068/without-pivot-usingjson")*/







  }

}
