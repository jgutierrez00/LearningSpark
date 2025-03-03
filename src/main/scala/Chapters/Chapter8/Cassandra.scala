package Chapters.Chapter8

import Chapters.Chapter
import org.apache.spark.sql._

import java.nio.file.{Files, Paths}

object Cassandra extends Chapter{

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val hostAddr = args(0)
    val keyspaceName = args(1)
    val tableName = args(2)

    def writeCountsToCassandra(updatedCountsDF: DataFrame, batchId: Long): Unit = {
      updatedCountsDF.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> tableName, "keyspace" -> keyspaceName))
        .mode("append")
        .save()
    }

    spark.conf.set("spark.cassandra.connection.host", hostAddr)

      if (Files.exists(Paths.get("/tmp/checkpoint"))) {
        val directory = new java.io.File("/tmp/checkpoint")
        directory.listFiles().foreach(_.delete())
        directory.delete()
      }

    val dataDF = spark.createDataFrame(Seq(("1234", 3), ("9876", 3))).toDF("userid", "item_count")

    for(_ <- 1 to 5) {
      dataDF.write
        .format("csv")
        .mode("overwrite")
        .option("path", "/tmp/data")
        .save()

      Thread.sleep(2000)
    }

    val streamingDF = spark.readStream
      .format("csv")
      .schema("userid STRING, item_count INT")
      .load("/tmp/data")


      val streamingQuery = streamingDF
        .writeStream
        .foreachBatch(writeCountsToCassandra _)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoint")
        .start()

      streamingQuery.awaitTermination()

  }
}
