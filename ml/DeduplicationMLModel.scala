package com.expedia.demos.ml



import org.apache.spark.ml.linalg.Vectors._
import org.apache.spark.ml.linalg.Vector
//import org.apache.spark.mllib.linalg.{BLAS =>
import org.apache.spark.sql.SparkSession


import org.apache.spark.sql.hive._
import org.apache.spark.sql.Row
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator._


object DeduplicationMLModel {


  def main(args:Array[String]): Unit =
  {
    val sparkSession = SparkSession.builder()
       .master("local[*]")
      .appName("DeduplicationMLModel")
      /*.config("fs.gs.project.id", "model-folio-818")
      .config("google.cloud.auth.service.account.enable", "true")
      .config("fs.gs.auth.service.account.json.keyfile", "<key_file>")*/
      .getOrCreate()



    import sparkSession.implicits._

    val training_data = sparkSession.read.format("csv")
      .option("header", true)
      .option("delimiter","\t")
      .load("/Users/pchitturi/Documents/homeaway/propertykeychain/datascience/training_files/vrbo_training.csv")
        .select("bedroom_diff", "bathroom_diff",
    "sleeps_diff", "source_attribute_area", "matched_attribute_area", "latlong_distance",
          "indoor_match_fraction", "image_match_fraction", "desc_similarity", "title_similarity","Label" )

   // println("Training Count: " + training_data.count())

    val testing_data = sparkSession.read.format("csv")
      .option("header", true)
      .option("delimiter","\t")
      .load("/Users/pchitturi/Documents/homeaway/propertykeychain/datascience/training_files/vrbo_testing.csv")
      .select("bedroom_diff", "bathroom_diff",
        "sleeps_diff", "source_attribute_area", "matched_attribute_area", "latlong_distance",
        "indoor_match_fraction", "image_match_fraction", "desc_similarity", "title_similarity","Label" )

   // println("Testing Count: " + testing_data.count())
    val vectors_rdd = training_data.map{row => val parts = row.mkString(",")
      val feature_Buffer = parts.split(",").map { ele => ele.toDouble }.toBuffer
            // println(feature_Buffer)
      val label = feature_Buffer.remove(10)
      val feature_Vec = Vectors.dense(feature_Buffer.toArray)
      (label, feature_Vec)
    }

    val vectors_rdd_testing = testing_data.map { row => val parts = row.mkString(",")
      val feature_Buffer = parts.split(",").map{ ele => ele.toDouble }.toBuffer
      val label = feature_Buffer.remove(10)

      val feature_Vec = Vectors.dense(feature_Buffer.toArray)
      (label, feature_Vec)
    }
    // DataFrame with dense vector as the column
    val test_features_Df = vectors_rdd_testing.toDF("label", "features")


    val final_features_Df = vectors_rdd.toDF("label", "features")

    val meta = NominalAttribute
      .defaultAttr
      .withName("label")
      .withValues("0.0", "1.0")
      .toMetadata

    val df_train = final_features_Df.withColumn("label", $"label".as("label", meta))

    val clf = new RandomForestClassifier()
      .setCheckpointInterval(1)
      .setFeatureSubsetStrategy("sqrt")
     /* .setMaxDepth(5)
      .setNumTrees(1500)*/
      .setLabelCol("label")

    val evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")

    val paramGrid = new ParamGridBuilder().addGrid(clf.maxDepth, Array(3,5,7,9)).addGrid(clf.numTrees, Array(300, 500,700, 1500,2700, 3500))
      .build()
    val pipeline = new Pipeline().setStages(Array(clf))


    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)

    val cv_model = cv.fit(df_train)

    val predictions_test = cv_model.transform(test_features_Df)



    val auc = evaluator.evaluate(predictions_test)

    // Confusion Matrix
    val cf_labels = predictions_test.select("prediction","label").rdd.map{row => (row.getAs[Double]("prediction"), row.getAs[Double]("label"))}
    val cfm_evaluator = new MulticlassMetrics(cf_labels)

    println("AUC: "+auc)
    println("Confusion Matrix: "+cfm_evaluator)

    // Weighted stats
    val accuracy = cfm_evaluator.accuracy
    println(s"Accuracy = $accuracy")
    println(s"Weighted precision: ${cfm_evaluator.weightedPrecision}")
    println(s"Weighted recall: ${cfm_evaluator.weightedRecall}")
    println(s"Weighted F1 score: ${cfm_evaluator.weightedFMeasure}")
    println(s"Weighted false positive rate: ${cfm_evaluator.weightedFalsePositiveRate}")



 //   xx.show(10, false)

    //println("Vrbo Training Data Count: " + vrboTrainingDf.count())
  }
}
