package Chapters.Chapter4

import Chapters.Chapter
import org.apache.spark.sql._

object Parquet extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val file = args(0)

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    spark.sql(
      s"""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
        |USING parquet
        |OPTIONS (
        |path '${file}')""".stripMargin)

    val df = spark.sql("SELECT * FROM us_delay_flights_tbl")
    df.show()

    df.write.format("parquet")
      .mode("overwrite")
      .option("compression", "snappy")
      .save(args(1))

    df.write
      .mode("overwrite")
      .saveAsTable("us_delay_flights_tbl_from_write")

    spark.sql("SELECT * FROM us_delay_flights_tbl_from_write").show()


  }
}
