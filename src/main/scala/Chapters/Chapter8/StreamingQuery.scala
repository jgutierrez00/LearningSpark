package Chapters.Chapter8

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object StreamingQuery extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val listener = new StreamingQueryListener() {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("Query started: " + event.id)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println("Query terminated: " + event.id)
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        println("Query made progress: " + event.progress)
      }
    }

    spark.streams.addListener(listener)

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

//    val filteredLines = lines.filter("isCorruptedUdf(value) = false")
//    val words = filteredLines.select(split(col("value"), "\\s").as("word"))
    val words = lines.select(split(col("value"), "\\s").as("word"))
    val counts = words.groupBy("word").count()

    val streamingQuery = counts.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime("1 second"))
      .option("checkpointLocation", "/tmp/checkpoint")
      .start()

    streamingQuery.awaitTermination()
  }
}
