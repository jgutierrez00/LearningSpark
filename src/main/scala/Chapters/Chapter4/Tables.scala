package Chapters.Chapter4

import Chapters.Chapter
import org.apache.spark.sql.SparkSession

object Tables extends Chapter {
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

//    spark.sql(
//      """CREATE TABLE manage_us_delay_flights_tbl (
//        |date STRING,
//        |delay INT,
//        |distance INT,
//        |origin STRING,
//        |destination STRING)
//        |""".stripMargin)

    // Directo usando el esquema y el csv

    val csv_file = args(0)

    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

    val flights_df = spark.read
      .option("header", true)
      .option("schema", schema)
      .csv(csv_file)
    flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

    // Unmanaged table

//    spark.sql(
//      s"""CREATE TABLE us_delay_flights_tbl (
//        |date STRING,
//        |delay INT,
//        |distance INT,
//        |origin STRING,
//        |destination STRING)
//        |USING csv OPTIONS (
//        |PATH '${args(0)}')
//        |""".stripMargin)

    // Con la API de DataFrame

//    flights_df.write
//      .options("path", "/tmp/data/us_flights_delay")
//      .saveAsTable("us_delay_flights_tbl")
  }
}