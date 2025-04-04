package Chapters.Chapter4

import Chapters.Chapter
import org.apache.spark.sql.SparkSession

object Views extends Chapter {
    override def run(spark: SparkSession, args: Array[String]): Unit = {

    spark.sql("CREATE DATABASE IF NOT EXISTS learn_spark_db")
    spark.sql("USE learn_spark_db")

    val csv_file = args(0)

    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

    val flights_df = spark.read
      .option("header", true)
      .option("schema", schema)
      .csv(csv_file)
    flights_df.write.saveAsTable("us_delay_flights_tbl")

    val df_sfo = spark.sql(
      """SELECT date, delay, origin, destination
        |FROM us_delay_flights_tbl WHERE origin = 'SFO'
        |""".stripMargin)

    val df_jfk = spark.sql(
      """SELECT date, delay, origin, destination
        |FROM us_delay_flights_tbl WHERE origin = 'JFK'
        |""".stripMargin)

    // Creamos una view global y temporal

    df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
    df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")


    spark.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view").show(10)
    spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view").show(10)

//    spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")
//    spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")

    }
}