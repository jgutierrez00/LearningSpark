package Chapters.Chapter5

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object CommonOperations extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val delayPaths = args(0)
    val airportsPath = args(1)

    val airports = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)

    airports.createOrReplaceTempView("airports_na")

    val delays = spark.read
      .option("header", "true")
      .csv(delayPaths)
      .withColumn("delay", expr("CAST(delay as int) as delay"))
      .withColumn("distance", expr("CAST(distance as int) as distance"))

    delays.createOrReplaceTempView("departureDelays")

    val foo = delays.filter(expr(
      """origin == 'SEA' AND destination == 'SFO' AND date like '01010%' AND delay > 0"""
    ))

    foo.createOrReplaceTempView("foo")
  }
}
