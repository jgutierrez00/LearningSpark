package Chapters.Chapter7

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Caching extends Chapter{

  var firstTime: Double = 0
  var secondTime: Double = 0

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    import spark.implicits._

    // Contamos el tiempo del primer acceso y lo comparamos con el segundo acceso.
    var startTime = System.nanoTime()
    val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id" )
    df.cache()
    df.count()
    var endtime = System.nanoTime()
    firstTime = (endtime - startTime).toDouble / 100000000
    println(s"First access: ${firstTime} miliseconds")
    startTime = System.nanoTime()
    df.count()
    endtime = System.nanoTime()
    secondTime = (endtime - startTime).toDouble / 100000000
    println(s"Second access: ${secondTime} miliseconds")

  }
}
