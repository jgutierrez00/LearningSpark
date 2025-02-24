package Chapters.Chapter4

import Chapters.Chapter
import org.apache.spark.sql._

object MetaData extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    val csv_file = args(0)

    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

    val flights_df = spark.read
      .option("header", true)
      .option("schema", schema)
      .csv(csv_file)
    flights_df.write.saveAsTable("us_delay_flights_tbl")

    spark.catalog.listDatabases().show()
    spark.catalog.listTables().show()
    spark.catalog.listColumns("us_delay_flights_tbl").show()

    val usFlighstsDF = spark.sql("SELECT * FROM us_delay_flights_tbl").show()
    val usFlightsDF2 = spark.table("us_delay_flights_tbl").show()

  }
}
