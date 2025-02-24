package Chapters.Chapter4

import Chapters.Chapter
import org.apache.spark.sql._

object DataReader extends Chapter{

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val fileParquet = args(0)
    val fileCsv = args(1)
    val fileJson = args(2)

    val df = spark.read.format("parquet").load(fileParquet)

    val df2 = spark.read.load(fileParquet)

    val df3 = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .load(fileCsv)

    val df4 = spark.read.format("json")
      .load(fileJson)

    df.show()
    df2.show()
    df3.show()
    df4.show()
  }
}
