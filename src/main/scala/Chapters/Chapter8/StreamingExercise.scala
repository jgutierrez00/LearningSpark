package Chapters.Chapter8

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object StreamingExercise extends Chapter{

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    // Extraccion

    val schema = new StructType()
      .add("id_venta", StringType, true)
      .add("producto", StringType, true)
      .add("cantidad", StringType, true)
      .add("precio_unitario", StringType, true)
      .add("fecha", StringType, true)
      .add("cliente", StringType, true)

    val dataDF = spark.readStream
      .format("csv")
      .schema(schema)
      .option("header", "true")
      .option("delimiter", ";")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .option("maxFilesPerTrigger", "1")
      .load(args(0))

    val dataWithTimeStampDF = dataDF.withColumn("timestamp", current_timestamp())

    // Transformacion

    val transformedDataDF = dataWithTimeStampDF.groupBy("cliente", "producto").count()

    // Load

    val queryConsole = transformedDataDF.writeStream
      .outputMode("complete")
      .format("console")
      .option("numRows", Int.MaxValue)
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    val queryDelta = transformedDataDF.writeStream
      .outputMode("complete")
      .format("delta")
      .option("checkpointLocation", "/tmp/checkpoint")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start(args(1))

    // Await terminations

    queryConsole.awaitTermination()
    queryDelta.awaitTermination()

  }
}
