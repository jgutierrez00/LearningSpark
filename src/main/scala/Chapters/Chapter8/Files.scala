package Chapters.Chapter8

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Files extends Chapter{

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val fileSchema = new StructType()
      .add("key", StringType)
      .add("value", StringType)

    val inputDF = spark.readStream
      .format("json")
      .schema(fileSchema)
      .load(args(0))

    val stremingQuery = inputDF.writeStream
      .format("parquet")
      .option("path", args(1))
      .option("checkpointLocation", args(2))
      .start()
  }

}
