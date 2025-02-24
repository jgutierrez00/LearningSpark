package Chapters.Chapter4

import Chapters.Chapter
import org.apache.spark.sql._

object JSON extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val file = args(0)

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    val df = spark.sql("SELECT * FROM us_delay_flights_tbl")

    df.write.format("json")
      .mode("overwrite")
      .option("compression", "snappy")
      .save(args(0))
  }
}
