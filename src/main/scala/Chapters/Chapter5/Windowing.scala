package Chapters.Chapter5

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Windowing extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    implicit val windowingEncoder: Encoder[WindowExercise] = Encoders.product[WindowExercise]

    val dataDS = spark.read
      .format("csv")
      .option("header", "true")
      .option("path", args(0))
      .load().as[WindowExercise]

    dataDS.show()

    val windowSpec = Window.orderBy(desc("power"))

    var modifiedDS = dataDS.withColumn("Rank", rank().over(windowSpec))
    modifiedDS = modifiedDS.withColumn("Dense Rank", dense_rank().over(windowSpec))
    modifiedDS = modifiedDS.withColumn("Row Number", row_number().over(windowSpec))

    modifiedDS.show()
  }
}
