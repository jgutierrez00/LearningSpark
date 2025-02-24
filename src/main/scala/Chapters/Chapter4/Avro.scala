/*
No hay nada en data/avro
 */
package Chapters.Chapter4

import Chapters.Chapter
import org.apache.spark.sql._

object Avro extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val df = spark.read.format("avro")
      .load(args(0))

    df.show(false)

    spark.sql(
      s"""CREATE OR REPLACE TEMPORARY VIEW episode_tbl
        |USING avro
        |OPTIONS (
        |path '${args(1)})'""".stripMargin)

    spark.sql("SELECT * FROM episode_tbl").show(false)

    df.write.format("avro")
      .mode("overwrite")
      .save(args(1))
  }
}
