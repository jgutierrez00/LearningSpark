package Chapters.Chapter8

import Chapters.Chapter
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql._

import java.nio.file.{Files, Paths}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.functions.current_timestamp

object Cassandra extends Chapter{

  var hostAddr: String = _
  var keyspaceName: String = _
  var tableName: String = _

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    hostAddr = args(0)
    keyspaceName = args(1)
    tableName = args(2)

    spark.conf.set("spark.cassandra.connection.host", hostAddr)
    spark.conf.set("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
    spark.conf.set("spark.cassandra.output.batch.size.bytes", "1000000")
    spark.conf.set("spark.cassandra.output.batch.size.rows", "1000")

    val dataDF = spark.createDataFrame(Seq(("1234", 3), ("9876", 3))).toDF("userid", "item_count")
      .withColumn("last_update_timestamp", current_timestamp().cast("timestamp"))

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
      .schema("userid STRING, item_count INT, last_update_timestamp TIMESTAMP")
      .load("/tmp/data")

      val streamingQuery = streamingDF
        .writeStream
        .foreachBatch(writeCountsToCassandra _)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoint")
        .start()

      streamingQuery.awaitTermination()

  }

  // Otra forma si foreachBatch no funciona o no se puede usar

  def open(spark: SparkSession, partitionId: Long, epochId: Long): Boolean = {
    val streamingDF = spark.readStream
      .format("csv")
      .schema("userid STRING, item_count INT, last_update_timestamp TIMESTAMP")
      .load("/tmp/data")

    return true
  }

  def process(spark: SparkSession, record: String, streamedDataFrame: DataFrame): Unit = {
    val streamingQuery = streamedDataFrame
      .writeStream
      .foreachBatch(writeCountsToCassandra _)
      .outputMode("append")
      .option("checkpointLocation", "/tmp/checkpoint")
      .start()

    streamingQuery.awaitTermination()
  }

  def close(errorOrNull: Throwable) = {

  }

  def writeCountsToCassandra(updatedCountsDF: DataFrame, batchId: Long): Unit = {
    updatedCountsDF.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> keyspaceName))
      .mode("append")
      .save()
  }
}
