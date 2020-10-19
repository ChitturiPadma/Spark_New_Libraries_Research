package com.expedia.demos

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

object SparkPipeDemo {


  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkPipeDemo")
      /*.config("fs.gs.project.id", "model-folio-818")
      .config("google.cloud.auth.service.account.enable", "true")
      .config("fs.gs.auth.service.account.json.keyfile", "<key_file>")*/
      .getOrCreate()

    import sparkSession.implicits._

    val scriptPath = "src/main/resources/predict_scores.py"
    val scriptName = "predict_scores.py"

    sparkSession.sparkContext.addFile(scriptPath)
     val rdd = sparkSession.sparkContext.makeRDD(Array(1,2,3,4,5))

   // val pipeRdd = rdd.pipe(SparkFiles.get(scriptName))
   val pipeRdd = rdd.pipe("python predict_scores.py")
    pipeRdd.foreach(println)
  }


}
