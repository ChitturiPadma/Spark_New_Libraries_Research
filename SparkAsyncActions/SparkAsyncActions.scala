package com.demos.spark.datasource.rest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import scala.util._

import scala.concurrent.ExecutionContext.Implicits.global
//import scala.util._

object SparkAsyncActions {

  def main(args:Array[String]): Unit = {

    val conf = new SparkConf().setMaster("spark://172.20.1.102:7077").setAppName("AsyncJobs_Spark")//.set("spark.scheduler.mode", "FAIR")
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(1 to 10).cache()
    val evenRdd = rdd1.filter(ele => ele%2 == 0 )

    val oddRdd = rdd1.filter(ele => ele%2 == 1)
    val newRdd = sc.makeRDD(11 to 20)

    // Jobs being executed sequentially
    evenRdd.collect.foreach(println)
    val oddNosCount = oddRdd.count
    println("#Odd number count..."+oddNosCount)

    // Jobs executing Asynchronously
    println("Job 1...")
    val future1Res   = evenRdd.collectAsync//.map{eleSeq => eleSeq.map(x => println("#Elemenet in the list.."+x)) }

    // future1Res.get().foreach(println)

    future1Res.onComplete{case trySeq:Try[Seq[Int]] => trySeq match {
      case Success(eleSeq) => eleSeq.foreach(println); sc.stop()
      case Failure(e) => println(e.getMessage); sc.stop()
    }}

    println("Job 2...")
    val future2Res = newRdd.countAsync
    future2Res.onComplete{case tryEle:Try[Long] => tryEle match {
      case Success(count) => println("Count is ..."+count); sc.stop()
      case Failure(e) => println(e.getMessage); sc.stop()
    } }

    future2Res.get()
    future1Res.get()
  }
}
