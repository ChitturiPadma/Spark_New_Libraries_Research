package com.expedia.demos

import org.apache.spark._
import org.apache.spark.sql._
import org.graphframes._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer

import java.time._
import java.time.format._

object Clickstream_DataJoinIssue {

  def main(args:Array[String]): Unit ={

    val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")


    val spark = SparkSession.builder()
     // .master("local[*]")
      .appName("ClickStream_DataJoinIssue")
      .getOrCreate()

    import spark.implicits._

    val originalStartDate = LocalDate.now.minusMonths(14)
    val originalEndDate = LocalDate.now.plusMonths(14)
    val datesToBeProcessed = ListBuffer[(String, String)]()
    generateDates(originalStartDate, originalEndDate, datesToBeProcessed )

   // val (lastYear, nextYear) = ("2017-12-17", "2018-02-17")

    datesToBeProcessed.foreach{
      case(lastYear:String, nextYear:String) =>

        val searchTermUuidDs = spark.sql(
          "select search_term_uuid,search_term_type,search_term_simple_name,search_term_full_name,search_term_country,is_strategic_destination,gaiaid from tier1_omnihub_graph.destination_profile_attributes")

        val stayBooking =spark.read.parquet("s3n://ha-prod-omnidata-us-east-1/"+"destination/stay_date_measure/booking").filter(col("stay_date")>=lastYear
          and col("stay_date")<nextYear)


        val staySupply = spark.read.parquet("s3n://ha-prod-omnidata-us-east-1/"+ "destination/stay_date_measure/supply")
          .withColumnRenamed("dateid", "dateid_staySupply")
          .withColumnRenamed("monthid", "monthid_staySupply")
          // .filter(to_date(col("stay_date")) >= newStartDate and to_date(col("stay_date")) < newEndDate)
          .filter(col("stay_date")>=lastYear
          and col("stay_date")<nextYear)

        val stayVisitor = spark.read.parquet("s3n://ha-prod-omnidata-us-east-1/" + "destination/stay_date_measure/search_unique_visitor")
          .withColumnRenamed("dateid", "dateid_stayVisitor")
          .withColumnRenamed("event_date", "eventdate_stayVisitor")
          // .filter(to_date(col("stay_date")) >= newStartDate and to_date(col("stay_date")) < newEndDate)
          .filter(col("stay_date")>=lastYear
          and col("stay_date")<nextYear)

        //val joinedStayDate = uniqStayDateList.join(stayBooking, seqColNames, "left_outer")

        val seqColNames: Seq[String] = Seq("search_term_uuid", "stay_date")
        val joinedStayDate = stayBooking
          .join(staySupply, seqColNames, "inner")
          .join(stayVisitor, seqColNames, "inner").repartition(300)

        // val finalJoinedStayDate = joinedStayDate.join(staysearchClickstream, seqColNames, "left_outer")

        val finalDs = joinedStayDate.join(searchTermUuidDs, Seq("Search_term_uuid"), "inner")
          .filter(col("stay_date").isNotNull).repartition(300)

        finalDs.write.mode(SaveMode.Append)
          .partitionBy()
          .parquet("s3n://ha-prod-omnidata-us-east-1/pachitturi/stayDateEnrichment_Output_withoutClickStream")


    }

   /* val stayBooking =spark.read.parquet("s3n://ha-prod-omnidata-us-east-1/"+"destination/stay_date_measure/booking").filter(col("stay_date")>=lastYear
      and col("stay_date")<nextYear)


    val staySupply = spark.read.parquet("s3n://ha-prod-omnidata-us-east-1/"+ "destination/stay_date_measure/supply")
      .withColumnRenamed("dateid", "dateid_staySupply")
      .withColumnRenamed("monthid", "monthid_staySupply")
      // .filter(to_date(col("stay_date")) >= newStartDate and to_date(col("stay_date")) < newEndDate)
      .filter(col("stay_date")>=lastYear
      and col("stay_date")<nextYear)

    val stayVisitor = spark.read.parquet("s3n://ha-prod-omnidata-us-east-1/" + "destination/stay_date_measure/search_unique_visitor")
      .withColumnRenamed("dateid", "dateid_stayVisitor")
      .withColumnRenamed("event_date", "eventdate_stayVisitor")
      // .filter(to_date(col("stay_date")) >= newStartDate and to_date(col("stay_date")) < newEndDate)
      .filter(col("stay_date")>=lastYear
      and col("stay_date")<nextYear)
*/
  }

  def generateDates(originalStart:LocalDate, originalEnd:LocalDate, datesToBeProcessedList:ListBuffer[(String, String)]   ):ListBuffer[(String, String)] = {
    //println("OriginalStart: " + originalStart)
    //println("OriginalEnd: " + originalEnd)
    val startDate = originalStart
    val endDate = startDate.plusMonths(2)
    if (startDate.isBefore(originalEnd)) {
      //println("In Ifcondition")
      datesToBeProcessedList.+=((startDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), endDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))))
      val newStartDate = endDate//.plusDays(1)
      generateDates(newStartDate, originalEnd, datesToBeProcessedList)
    }
    else
      datesToBeProcessedList
  }

}
