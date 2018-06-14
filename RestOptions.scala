package com.demos.spark.datasource.rest


import java.sql.{Connection, DriverManager}
import java.util.{Locale, Properties}


/**
  * Options for the REST data source.
  */


class RestOptions(
                   // @transient private val parameters: CaseInsensitiveMap[String])
                   @transient private val parameters: Map[String, String])
  extends Serializable {

  import RestOptions._

  // def this(parameters: Map[String, String]) = this(parameters)
  def this(url: String, input: String, parameters: Map[String, String]) = {
    // this(CaseInsensitiveMap(parameters ++ Map(
    this(parameters ++ Map(
      RestOptions.REST_URL -> url,
      RestOptions.REST_INPUT -> input))
  }

  val asProperties: Properties = {
    val properties = new Properties()
    parameters.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  /*
     Required Parameters
  */

  require(parameters.isDefinedAt(REST_URL), s"Option '$REST_URL' is required.")
  require(parameters.isDefinedAt(REST_INPUT), s"Option '$REST_INPUT' is required.")

  val url = parameters(REST_URL)
  val input = parameters(REST_INPUT)

  /*
     Optional Parameters
  */


  val inputType = parameters.getOrElse(REST_INPUT_TYPE, "tableName")
  val authType = parameters.getOrElse(REST_AUTH_TYPE, "Basic")
  val userId = parameters.getOrElse(REST_USER_ID, "")
  val userPassword = parameters.getOrElse(REST_USER_PASSWORD, "")
  val method = parameters.getOrElse(REST_METHOD, "POST")
  val postInputFormat = parameters.getOrElse(REST_POST_INPUT_FORMAT, "json")
  val inputKeys = parameters.getOrElse(REST_INPUT_KEYS, "")
  val outputFormat = parameters.getOrElse(REST_OUTPUT_FORMAT, "json")
  val readTimeout = parameters.getOrElse(REST_READ_TIMEOUT, "5000")
  val connectionTimeout = parameters.getOrElse(REST_CONNECTION_TIMEOUT, "1000")
  val inputStrRecordDelimeter = parameters.getOrElse(REST_INPUT_STR_RECORD_DELIMETER, ";")
  val inputStrFieldDelimeter = parameters.getOrElse(REST_INPUT_STR_FIELD_DELIMETER, "#")
  val inputPartitions = parameters.getOrElse(REST_INPUT_PARTITIONS, "2")
  val includeInputsInOutput = parameters.getOrElse(REST_INPUT_INCLUDE, "Y")
  val oauthConsumerKey = parameters.getOrElse(REST_OAUTH1_CONSUMER_KEY, "")
  val oauthConsumerSecret = parameters.getOrElse(REST_OAUTH1_CONSUMER_SECRET, "")
  val oauthToken = parameters.getOrElse(REST_OAUTH1_TOKEN, "")
  val oauthTokenSecret = parameters.getOrElse(REST_OAUTH1_TOKEN_SECRET, "")
  val callStrictlyOnce = parameters.getOrElse(REST_CALL_STRICTLY_ONCE, "N")
  val schemaSamplePcnt = parameters.getOrElse(REST_SCHEMA_SAMPLE_PCNT, "30")

}


object RestOptions {

  private val restOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    restOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val REST_URL = newOption("url")
  val REST_INPUT_TYPE = newOption("inputType")
  val REST_INPUT = newOption("input")
  val REST_POST_INPUT_FORMAT = newOption("postInputFormat")
  val REST_INPUT_KEYS = newOption("inputKeys")
  val REST_USER_ID = newOption("userId")
  val REST_USER_PASSWORD = newOption("userPassword")
  val REST_METHOD = newOption("method")
  val REST_OUTPUT_FORMAT = newOption("outputFormat")
  val REST_READ_TIMEOUT = newOption("readTimeOut")
  val REST_CONNECTION_TIMEOUT = newOption("connectionTimeOut")
  val REST_INPUT_STR_RECORD_DELIMETER = newOption("inputStrRecordDelimeter")
  val REST_INPUT_STR_FIELD_DELIMETER = newOption("inputStrFieldDelimeter")
  val REST_INPUT_PARTITIONS = newOption("inputPartitions")
  val REST_INPUT_INCLUDE = newOption("includeInputsInOutput")
  val REST_AUTH_TYPE = newOption("authType")
  val REST_OAUTH1_CONSUMER_KEY = newOption("oauthConsumerKey")
  val REST_OAUTH1_CONSUMER_SECRET = newOption("oauthConsumerSecret")
  val REST_OAUTH1_TOKEN = newOption("oauthToken")
  val REST_OAUTH1_TOKEN_SECRET = newOption("oauthTokenSecret")
  val REST_CALL_STRICTLY_ONCE = newOption("callStrictlyOnce")
  val REST_SCHEMA_SAMPLE_PCNT = newOption("schemaSamplePcnt")
}
