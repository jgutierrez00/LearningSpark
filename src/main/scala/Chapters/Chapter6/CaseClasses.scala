package Chapters.Chapter6

import Chapters.Chapter
import org.apache.spark.sql._

object CaseClasses extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    // Creamos el encoder en base a la case class
    implicit val bloggersEncoder: Encoder[Bloggers] = Encoders.product[Bloggers]

    // Cargamos el dataset indicando el encoder
    val bloggersDS = spark.read
      .format("json")
      .option("path", args(0))
      .load().as[Bloggers]

    bloggersDS.show()
  }
}
