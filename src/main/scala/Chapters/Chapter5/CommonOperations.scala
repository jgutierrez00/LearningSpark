package Chapters.Chapter5

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object CommonOperations extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

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

    spark.sql("""SELECT * FROM foo""").show(10)
    spark.sql("""SELECT * FROM airports_na""").show(10)
    spark.sql("""SELECT * FROM departureDelays""").show(10)

    // Uniones

    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date like '01010%' AND delay > 0""")).show(10)

    // Joins

    foo.join(
      airports.as("air"),
      expr("origin == air.IATA")
    ).select("City", "State", "date", "delay", "distance", "destination").show(10)


    // Windowing functions (es necesario habilitar Hive support)

//    spark.sql("""CREATE TABLE depatureDelaysWindow AS
//      |SELECT origin, destination, SUM(delay) as TotalDelays
//      |FROM departureDelays
//      |WHERE origin IN ('SEA', 'SFO', 'JFK')
//      |AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
//      |GROUP BY origin, destination;""".stripMargin)
//
//
//    spark.sql("""SELECT * FROM depatureDelaysWindow""").show(10)
//
//    spark.sql("""SELECT origin, destination, SUM(TotalDelays) as TotalDelays
//      |FROM depatureDelaysWindow
//      |WHERE origin = '[ORIGIN]'
//      |GROUP BY origin, destination
//      |ORDER BY SUM(TotalDelays) DESC
//      |LIMIT 3""".stripMargin).show(10)
//
//    spark.sql(
//      """SELECT origin, destination, TotalDelays, rank
//        |FROM (
//        |SELECT origin, destination, TotalDelays, dense_rank()
//        |OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
//        |FROM depatureDelaysWindow) t
//        |WHERE rank <= 3""".stripMargin).show()
  }
}
