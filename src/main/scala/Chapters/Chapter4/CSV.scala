package Chapters.Chapter4

import Chapters.Chapter
import org.apache.spark.sql._

object CSV extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val file = args(0)
    val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"

    val df = spark.read.format("csv")
      .option("schema", schema)
      .option("header", "true")
      .option("mode", "FAILFAST")   // Termina si hay algun error
      .option("nullValue", "")      // Reemplaza nulls con valores vacios
      .load(file)

    spark.sql(
      s"""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
        |USING csv
        |OPTIONS (
        |path '${file}',
        |header 'true',
        |inferSchema 'true',
        |mode 'FAILFAST')""".stripMargin)

    spark.sql("SELECT * FROM us_delay_flights_tbl").show(10)

    df.write.format("csv")
      .mode("overwrite")
      .save(args(1))
  }
}
