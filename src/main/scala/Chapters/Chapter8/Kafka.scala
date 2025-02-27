package Chapters.Chapter8

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.streaming._

object Kafka extends Chapter{

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val inputDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topico-prueba")
      .option("startingOffsets", "earliest")
      .load()

    val query = inputDF.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  } 
}
