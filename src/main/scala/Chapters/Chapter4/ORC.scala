package Chapters.Chapter4

import Chapters.Chapter
import org.apache.spark.sql._

object ORC extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val file = args(0)

    val df = spark.read.format("orc")
      .load(file)

    spark.sql(
      s"""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
        |USING orc
        |OPTIONS (
        |path '${file}')""".stripMargin)

    spark.sql("SELECT * FROM us_delay_flights_tbl").show(false)

    df.write.format("orc")
      .mode("overwrite")
      .option("compression", "snappy")
      .save(args(1))
  }
}
