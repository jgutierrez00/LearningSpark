package Chapters.Chapter5

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Modifications extends Chapter{
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

    // Añadir columnas

    val foo2 = foo.withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))
    foo2.show()

    // Eliminar columnas

    val foo3 = foo2.drop("delay")
    foo3.show()

    // Renombrar columnas
    val foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()

    val foo5 = delays.select(
      col("destination"),
      expr("CAST(SUBSTRING(date, 0, 2) AS INT) AS month"),
      col("delay")
    )
    foo5.show()

    val pivotDF = delays
      .select(
        col("destination"),
        expr("CAST(SUBSTRING(date, 0, 2) AS int) AS month"),
        col("delay")
      ).filter(col("origin") === "SEA")
      .groupBy("destination")
      .pivot("month", Seq(1, 2))
      .agg(
        expr("CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay"),
        expr("MAX(delay) AS MaxDelay")
      )
      .orderBy("destination")

    pivotDF.show()

  }
}
