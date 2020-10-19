package com.expedia.demos

import java.io._

//import com.samelamin.spark.bigquery._
import org.apache.spark.sql.SparkSession

object SparkBigQuerySpotify {

  def mainin(args:Array[String]): Unit =
  {
    val sparkSession = SparkSession.builder()
      //.master("local[*]")
      .appName("SparkBigQuerySpotify")
      /*.config("fs.gs.project.id", "model-folio-818")
      .config("google.cloud.auth.service.account.enable", "true")
      .config("fs.gs.auth.service.account.json.keyfile", "<key_file>")*/
      .getOrCreate()

   /* sparkSession.sqlContext.setBigQueryProjectId("model-folio-818")
    sparkSession.sqlContext.setBigQueryGcsBucket("gs://omnidata-baklava/")

    val serviceAccountCredentialsJson = File.createTempFile("Homeaway-GAFastAccess", ".json")
    var line = null

    //Read the credentials JSON file from resources folder and write it to a temp file Homeaway-GAFastAccess.json
   //log.info("##### Reading credentials JSON file from resources folder and create temporary bash file")
    val bwCredentialsJson = new BufferedWriter(new FileWriter(serviceAccountCredentialsJson))
    val credentialFile = this.vaultServices.getSecretString(this.vaultCredentialPath)
    val isCredentialsJson = new ByteArrayInputStream(credentialFile.getBytes)
    //InputStream isCredentialsJson = this.getClass().getClassLoader().getResourceAsStream("HomeAway-GAFastAccess-c73ac1cf76e9.json");
    val credentialsJsonContent = new BufferedReader(new InputStreamReader(isCredentialsJson))
    while ( {
      (line = credentialsJsonContent.readLine) != null
    }) {
   //   log.info(line)
      bwCredentialsJson.write(line)
      bwCredentialsJson.newLine()
    }
    bwCredentialsJson.close()

    sparkSession.sqlContext.setGcpJsonKeyFile("Homeaway-GAFastAccess.json")


    val df = sparkSession.sqlContext.read.format("com.samelamin.spark.bigquery")
      .option("tableReferenceSource","plx.google:baklava.ha_destinations_daily").load()

*/
  }

}
