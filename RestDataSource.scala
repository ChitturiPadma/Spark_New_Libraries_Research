package com.expedia.spark.datasource.rest

import org.apache.spark.SparkContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.SQLContext


/*
   This is the implementation of the interface needed
   for DataFrameReader class to load this Data Source
*/

class RestDataSource extends RelationProvider
  with DataSourceRegister {

  override def shortName(): String = "rest"

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {

    import RestOptions._

    val restOptions = new RestOptions(parameters)

    RestRelation(restOptions)(sqlContext.sparkSession)
  }
}
