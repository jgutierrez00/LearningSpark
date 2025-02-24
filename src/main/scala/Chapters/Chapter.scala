package Chapters

import org.apache.spark.sql.SparkSession

trait Chapter {
  def run(spark: SparkSession, args: Array[String] = Array()): Unit
}