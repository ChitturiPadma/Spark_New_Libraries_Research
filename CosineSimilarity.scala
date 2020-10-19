package com.expedia.demos.ml

import org.apache.spark.ml.linalg.Vectors._
import org.apache.spark.ml.linalg.Vector
//import org.apache.spark.mllib.linalg.{BLAS =>
import org.apache.spark.sql.SparkSession


object CosineSimilarity {

  def replaceTitle(title:String):String =  title.replace("\\", "").replace("\"", "")
  def cleanText(text:String):String =
  {
    text.toLowerCase()
      .replaceAll("&#13;"," ")
      .replaceAll("\\.", "\\. ")
      .replaceAll("nbsp", " ")
      .replaceAll("  "," ")
  }


  /*def cosine_similarity(X:Vector, Y:Vector):Double =
  {
    val denom = norm(X,2) * norm(Y,2)

    val XMlibVector = org.apache.spark.mllib.linalg.Vectors.fromML(X)
    val YMlibVector = org.apache.spark.mllib.linalg.Vectors.fromML(Y)

    val dotproduct = org.apache.spark.mllib.linalg.BLAS.dot(XMlibVector,YMlibVector )

    if(denom == 0.0) -1.0  else  dotproduct / denom.toDouble
  }*/

  def main(args:Array[String]): Unit =
  {
    val sparkSession = SparkSession.builder()
     // .master("local[*]")
      .appName("CosineSimilarity")
      /*.config("fs.gs.project.id", "model-folio-818")
      .config("google.cloud.auth.service.account.enable", "true")
      .config("fs.gs.auth.service.account.json.keyfile", "<key_file>")*/
      .getOrCreate()



    import sparkSession.implicits._
  }
}
