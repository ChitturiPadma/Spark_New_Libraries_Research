package com.expedia.spark.datasource.rest


import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object RestDataSource_Usage {

  def main(args:Array[String]): Unit ={


    val sess = SparkSession.builder().appName("CustomDataSource_Usage").master("local[*]")
      .config("spark.driver.host","localhost").getOrCreate()

    // Create the target url string for Soda API for data source
    //val uri = "http://place-service-stage.us-aus-1-dts.slb.dts.vxe.away.black/geo/v1/places"

    val uri = "https://maps.googleapis.com/maps/api/geocode/json"

    val input1 = "39.9525839,-75.1652215"
    val input2 = "fb833e81-92e7-4117-977a-e660261291be"
    val input3 = "43deefc3-766f-4445-828b-1d2bc543198d"
    val input4 = "2268a674-164b-4a74-ae6a-537d948d1b28"
    val input5 = "39c3daf7-d780-4dcb-9975-a45346ec2183"
    val input6 = "8a710db1-4594-4328-b1e1-e2fa3b72f9f5"



    // Now we create a RDD using these input parameter values

    //val inputRdd = sess.sparkContext.parallelize(Seq(input1, input2, input3))
   //  val inputRdd = sess.sparkContext.parallelize(Seq(input1, input2,input3, input4, input5, input6 ))

    val inputRdd = sess.sparkContext.parallelize(Seq(input1))

    // Next we need to create the DataFrame specifying specific column names that match the field names we wish to filter on
    //val inputKey1 = "region"
    val inputKey1 = "latlng"


    import sess.implicits._

    //val Df = inputRdd.toDF(inputKey1, inputKey2)
    val df = inputRdd.toDF(inputKey1)

    // And we create a temporary table now using the df
    df.createOrReplaceTempView("inputtbl")

    // Now we create the parameter map to pass to the REST Data Source.

    val parmg = Map("url" -> uri, "input" -> "inputtbl", "method" -> "GET", "readTimeout" -> "10000", "connectionTimeout" -> "2000", "partitions" -> "10","userId" -> "omnihub", "userPassword" -> "omnihub", "callStrictlyOnce" -> "Y")

    // Now we create the Dataframe which contains the result from the call to the  API for the 3 different input data points
    val resultsDf = sess.read.format("com.expedia.spark.datasource.rest.RestDataSource").options(parmg).load()

    resultsDf.printSchema()

    // Now we are ready to apply SQL or any other processing on teh results

   // resultsDf.createOrReplaceTempView("resultstbl")

    //resultsDf.printSchema

    //val fetchedRes = sess.sql("select * from resultstbl")
    //println("Result Count...."+fetchedRes.count)

   // fetchedRes.show(53, false)

    /*val fetchedRes = resultsDf.withColumn("explodedCol",explode(resultsDf("output.adjacents")))
      .select($"explodedCol.place.name.full".alias("nearest_searchterm"), $"explodedCol.place.uuid".alias("nearest_uuid"), $"uuid")

    val w = Window.partitionBy($"uuid").orderBy(lit(1))

    val dfTop = fetchedRes.withColumn("rn", row_number.over(w)).where($"rn" === 1)

   dfTop.show(1000, false)*/


    //fetchedRes.printSchema()
/*   val firstRes = fetchedRes.first()

    println(firstRes)*/




  }
}
