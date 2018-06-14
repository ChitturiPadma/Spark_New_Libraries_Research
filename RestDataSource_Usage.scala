package com.demos.spark.datasource.rest


import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object RestDataSource_Usage {

  def main(args:Array[String]): Unit ={


    val sess = SparkSession.builder().appName("CustomDataSource_Usage").master("local[*]")
      .config("spark.driver.host","localhost").getOrCreate()

    // Create the target url string for REST API for data source
    
    val uri = "https://maps.googleapis.com/maps/api/geocode/json"

    val input1 = "39.9525839,-75.1652215"
    val input2 = "33.0198441,-96.6988831"
    val input3 = "48.9890709,2.258451"
   
    // Now we create a RDD using these input parameter values

    val inputRdd = sess.sparkContext.parallelize(Seq(input1, input2, input3))
   
    // Next we need to create the DataFrame specifying specific column names that match the field names we wish to filter on
    val inputKey1 = "latlng"
    import sess.implicits._

    //val Df = inputRdd.toDF(inputKey1, inputKey2)
    val df = inputRdd.toDF(inputKey1)

    // And we create a temporary table now using the df
    df.createOrReplaceTempView("inputtbl")

    // Now we create the parameter map to pass to the REST Data Source.

    val parmg = Map("url" -> uri, "input" -> "inputtbl", "method" -> "GET", "readTimeout" -> "10000", "connectionTimeout" -> "2000", "partitions" -> "10","userId" -> "omnihub", "userPassword" -> "omnihub", "callStrictlyOnce" -> "Y")

    // Now we create the Dataframe which contains the result from the call to the  API for the 3 different input data points
    val resultsDf = sess.read.format("com.demos.spark.datasource.rest.RestDataSource").options(parmg).load()

    resultsDf.printSchema()

    resultsDf.show(10, false)




  }
}
