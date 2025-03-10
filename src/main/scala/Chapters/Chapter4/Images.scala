/*
No reconoce "image" como un datasource. Mllib esta como dependencia.
 */

package Chapters.Chapter4

import Chapters.Chapter
import org.apache.spark.sql._

object Images extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val imageDir = args(0)

    // Cargamos las imágenes en un DataFrame
    val imagesDF = spark.read.format("image")
      .load(imageDir)

    imagesDF.printSchema

    imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show(5, false)
  }
}
