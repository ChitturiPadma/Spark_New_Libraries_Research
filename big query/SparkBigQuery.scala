package com.expedia.demos

import org.apache.spark.sql.SparkSession
/*import com.google.api.services.bigquery.model.JsonObject

import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryFileFormat, GsonBigQueryInputFormat}
import com.google.cloud.hadoop.io.bigquery.output.{BigQueryOutputConfiguration, IndirectBigQueryOutputFormat}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat*/

object SparkBigQuery {

  def main(args:Array[String]): Unit =
  {

    val sparkSession = SparkSession.builder()
      //.master("local[*]")
      .appName("SparkBigQuery")
      .config("fs.gs.project.id", "model-folio-818")
      .config("google.cloud.auth.service.account.enable", "true")
      .config("fs.gs.auth.service.account.json.keyfile", "<key_file>")
      .getOrCreate()

   /* val fullyQualifiedInputTableId ="plx.google:baklava.ha"
    val projectId = "model-folio-818"
    val bucket = "omnidata-baklava"

    @transient
    val conf = sparkSession.sparkContext.hadoopConfiguration

    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    conf.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket)
    BigQueryConfiguration.configureBigQueryInput(conf, fullyQualifiedInputTableId)
    // Output parameters.
    val outputTableId = projectId + ":wordcount_dataset.wordcount_output"
    // Temp output bucket that is deleted upon completion of job.
    val outputGcsPath = ("gs://" + bucket + "/hadoop/tmp/bigquery/wordcountoutput")

    // Output configuration.
    // Let BigQuery auto-detect output schema (set to null below).
    BigQueryOutputConfiguration.configureWithAutoSchema(
      conf,
      outputTableId,
      outputGcsPath,
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
      classOf[TextOutputFormat[_,_]])

    conf.set("mapreduce.job.outputformat.class",
      classOf[IndirectBigQueryOutputFormat[_,_]].getName)

    // Truncate the table before writing output to allow multiple runs.
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY,
      "WRITE_TRUNCATE")

    val tableData = sparkSession.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[GsonBigQueryInputFormat],
      classOf[LongWritable],
      classOf[JsonObject])
*/




  }

}
