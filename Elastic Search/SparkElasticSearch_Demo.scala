package com.expedia.demos

import org.elasticsearch.spark._
import org.apache.spark.sql._

object SparkElasticSearch_Demo {

  case class Student(id:Int, name:String, marks:Double)
   def main(args:Array[String]): Unit ={

      val sess = SparkSession.builder().appName("SparkElasticSearchDemo")
                .master("local[*]")
                .config("spark.es.index.auto.create", "true")
        .config("spark.es.nodes", "127.0.0.1")
        .config("spark.es.port", "9200")
        .config("spark.es.nodes.wan.only", "true")
        .config("spark.es.net.http.auth.user","")
        .config("spark.es.net.http.auth.pass", "")
        .config("spark.driver.host","localhost")
        .getOrCreate()

     val studentRdd = sess.sparkContext.makeRDD(Array((101, "Padma", 90), (102, "Kumar", 95), (103, "Vikas", 85)))

     val studentRdd_structured = studentRdd.map{case(id:Int, name:String, marks:Int) => Student(id, name, marks.toDouble) }

     studentRdd_structured.saveToEs("student/details")


   }
}
