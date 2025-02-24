package Chapters.Chapter4

import Chapters.Chapter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BasicQueries extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val csvFile = args(0)

    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)

    df.createOrReplaceTempView("us_delay_flights_tbl")

    spark.sql(
      """SELECT distance, origin, destination
        |FROM us_delay_flights_tbl WHERE distance > 1000
        |ORDER BY distance DESC
        |""".stripMargin).show(10)

    spark.sql(
      """SELECT date, delay, origin, destination
        |FROM us_delay_flights_tbl
        |WHERE delay > 120 AND origin = 'SFO' AND destination = 'ORD'
        |ORDER BY delay DESC
        |""".stripMargin).show(10)

    spark.sql(
      """SELECT delay, origin, destination,
        |CASE
        |WHEN delay > 360 THEN 'Very Long Delays'
        |WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
        |WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
        |WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delays'
        |WHEN delay = 0 THEN 'No Delays'
        |ELSE 'Early'
        |END  AS Flight_Delays
        |FROM us_delay_flights_tbl
        |ORDER BY origin, delay DESC
        |""".stripMargin).show(10)


      // Otra forma de hacer queries
      df.select("distance", "origin", "destination")
        .where(col("distance") > 1000)
        .orderBy("distance DESC").show(10)
  }
}
