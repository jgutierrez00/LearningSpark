package Chapters.Chapter9

import Chapters.Chapter
import org.apache.spark.sql._

object DeltaLake extends Chapter{

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val sourcePath = args(0)
    val deltaPath = args(1)

    spark.sql("CREATE DATABASE learning_spark_db")
    spark.sql("USE learning_spark_db")

    spark.read
      .format("parquet")
      .load(sourcePath)
      .write
      .format("delta")
      .save(deltaPath)

    spark.read
      .format("delta")
      .load(deltaPath)
      .createOrReplaceTempView("loans_delta")

    spark.sql("SELECT * FROM loans_delta").show()

  }
}
