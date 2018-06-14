package com.expedia.spark.datasource.rest


import java.io._
import java.net.{HttpURLConnection, URL, URLEncoder}
import java.security._
import javax.net.ssl.{SSLContext, SSLSocketFactory, TrustManagerFactory}

import scala.annotation.switch
import scala.collection.mutable.ArrayBuffer

import scalaj.http.{Http, HttpOptions, Token}


/**
  * This object contains all utility functions for reading/writing data from/to remote rest service
  */

object RestConnectorUtil {


  def callRestAPI(uri: String,
                  data: String,
                  method: String,
                  oauthCredStr: String,
                  userCredStr: String,
                  connStr: String,
                  contentType: String,
                  respType: String): Any = {


    val httpc = (method: @switch) match {
      case "GET" => Http(addQryParmToUri(uri, data)).header("contenty-type",
        "application/x-www-form-urlencoded")
      case "PUT" => Http(uri).put(data).header("content-type", contentType)
      case "DELETE" => Http(uri).method("DELETE")
      case "POST" => Http(uri).postData(data).header("content-type", contentType)
    }

    val conns = connStr.split(":")
    val connProp = Array(conns(0).toInt, conns(1).toInt)

    val httpConn = httpc.timeout(connTimeoutMs = connProp(0),
      readTimeoutMs = connProp(1))

    httpConn.option(HttpOptions.allowUnsafeSSL)

    val httpcAuth = if (oauthCredStr == "") { if (userCredStr == "") httpConn else {
        val usrCred = userCredStr.split(":")
        httpConn.auth(usrCred(0), usrCred(1))
      }
    }
    else {
      val oauthd = oauthCredStr.split(":")
      val consumer = Token(oauthd(0), oauthd(1))
      val accessToken = Token(oauthd(2), oauthd(3))
      httpConn.oauth(consumer, accessToken)
    }

    val resp = (respType : @switch) match {
      case "BODY" => httpcAuth.asString.body
      case "BODY-BYTES" => httpcAuth.asBytes.body
      case "BODY-STREAM" => getBodyStream(httpcAuth)
      case "CODE" => httpcAuth.asString.code
      case "HEADERS" => httpcAuth.asString.headers
      case "LOCATION" => httpcAuth.asString.location.mkString(" ")
    }
    resp
  }

 /* private def addQryParmToUri(uri: String, data: String) : String = {

    val newUri = uri + data
    val queryParamStr = if (newUri contains "?") newUri + URLEncoder.encode("q")+ "=" + URLEncoder.encode("type=\"locality\"") + "&" + URLEncoder.encode("sorts")+ "=" + URLEncoder.encode("absolutePopularity") else newUri + "?" + URLEncoder.encode("q")+ "=" + URLEncoder.encode("type=\"locality\"") + "&" + URLEncoder.encode("sorts")+ "=" + URLEncoder.encode("absolutePopularity")
    queryParamStr
  }*/

  private def addQryParmToUri(uri: String, data: String) : String = {
    val newUri = if (uri contains "?") uri + "&" + data else uri + "?" + data

    newUri + "&" + URLEncoder.encode("key") + "=" + URLEncoder.encode("AIzaSyBdRR0Icp8r6J1w0HLyUdYaj_fj6CHxzkc")
  }

  private def convertToQryParm(data: String) : List[(String, String)] = {
    data.substring(1, data.length - 1).split(",").map(_.split(":"))
      .map{ case Array(k, v) => (k.substring(1, k.length-1), v.substring(1, v.length-1))}
      .toList
  }

  private def getBodyStream(httpReq: scalaj.http.HttpRequest) : InputStream = {

    val conn = (new URL(httpReq.urlBuilder(httpReq))).openConnection.asInstanceOf[HttpURLConnection]

    HttpOptions.method(httpReq.method)(conn)

    httpReq.headers.reverse.foreach{ case (name, value) =>
      conn.setRequestProperty(name, value)
    }

    httpReq.options.reverse.foreach(_(conn))

    httpReq.connectFunc(httpReq, conn)

    conn.getInputStream

  }

  def prepareJsonInput(keys: Array[String], values: Array[String]) : String = {

    val outArrB = keys.zipWithIndex.map{case(keyEle, index) => "\"" + keyEle +"\":\"" +values(index)+"\""}
    "{" + outArrB.mkString(",") + "}"
  }


  //def prepareTextInput(keys: Array[String], values: Array[String]) : String =  "/"+URLEncoder.encode(values.head)+"/adjacent"

  def prepareTextInput(keys: Array[String], values: Array[String]) : String = {

    val outString = keys.zipWithIndex.map{case(keyEle, index) =>URLEncoder.encode(keys(index)) +"=" + URLEncoder.encode(values(index)) }
    outString.head

  }
  def prepareJsonOutput(keys: Array[String], values: Array[String], resp: String) : String = {

    val outArrB = keys.zipWithIndex.map{case(keyEle, index) => "\"" + keyEle +"\":\"" +values(index)+"\""}
    "{" + outArrB.mkString(",") +  ",\"output\":" + resp + "}"
  }

}