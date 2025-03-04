package Chapters.Chapter7

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Caching extends Chapter{

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    import spark.implicits._

    var startTime = System.nanoTime()
    val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id" )
    df.cache()
    df.count()
    var endtime = System.nanoTime()
    println(s"First access: ${(endtime - startTime).toDouble / 100000000} miliseconds")
    startTime = System.nanoTime()
    df.count()
    endtime = System.nanoTime()
    println(s"Second access: ${(endtime - startTime).toDouble / 100000000} miliseconds")

  }
}
