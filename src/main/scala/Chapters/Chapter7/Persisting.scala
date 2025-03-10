package Chapters.Chapter7

import Chapters.Chapter
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Persisting extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    import spark.implicits._

    // Contamos el tiempo del primer acceso y lo comparamos con el segundo acceso.
    val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
    df.persist(StorageLevel.DISK_ONLY)

    var startTime = System.nanoTime()
    df.count()
    var endTime = System.nanoTime()
    println(s"First count took ${(endTime - startTime) / 1000000} miliseconds")

    startTime = System.nanoTime()
    df.count()
    endTime = System.nanoTime()
    println(s"Second count took ${(endTime - startTime) / 1000000} miliseconds")
  }
}
